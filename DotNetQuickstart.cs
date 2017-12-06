// Copyright (c) Microsoft Corporation
//
// Companion project to the following article:
// https://docs.microsoft.com/en-us/azure/batch/quick-run-dotnet

using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using System.IO;
using System.Threading.Tasks;

namespace BatchDotNetQuickstart
{
    public class Program
    {
        // Update the Batch and Storage account credential strings below with the values unique to your accounts.
        // These are used when constructing connection strings for the Batch and Storage client objects.

        // Batch account credentials
        private const string BatchAccountName = "mybatchaccount";
        private const string BatchAccountKey = "gMSB4M7NW79/djOu/33KbKEPuh7nVIsk2V17dqt2voj0kFLbpQJenDqDpaWLDi7RKpF+wEy4oOSOGSbkxVPLOQ==";
        private const string BatchAccountUrl = "https://mybatchaccount.westus2.batch.azure.com";

        // Storage account credentials
        private const string StorageAccountName = "mybatchstorage121";
        private const string StorageAccountKey = "ST+B5L0VOvv/diqJPVBYMZmR83oS//uncqA590SxjutFNT0THLYqJn72TcM8/e4B0m3Od4WsUHkHRxgI3L8WHw==";
        private const string PoolId = "DotNetQuickstartPool";
        private const string JobId = "DotNetQuickstartJob";
       
        static void Main(string[] args)
        {

            if (String.IsNullOrEmpty(BatchAccountName) || String.IsNullOrEmpty(BatchAccountKey) || String.IsNullOrEmpty(BatchAccountUrl) ||
                String.IsNullOrEmpty(StorageAccountName) || String.IsNullOrEmpty(StorageAccountKey))
            {
                throw new InvalidOperationException("One ore more account credential strings have not been populated. Please ensure that your Batch and Storage account credentials have been specified.");
            }
            

            // Construct the Storage account connection string
            string storageConnectionString = String.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}",
                                                            StorageAccountName, StorageAccountKey);

            // Retrieve the storage account
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the blob client, for use in obtaining references to blob storage containers
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Use the blob client to create the containers in Azure Storage if they don't yet exist
            const string inputContainerName = "input";
            //await CreateContainerIfNotExistAsync(blobClient, inputContainerName);

            CloudBlobContainer container = blobClient.GetContainerReference(inputContainerName);

            container.CreateAsync();

            // The collection of data files that are to be processed by the tasks
            List<string> inputFilePaths = new List<string>
            {
                @"..\..\taskdata0.txt",
                @"..\..\taskdata1.txt",
                @"..\..\taskdata2.txt"
            };

            // Upload the data files to Azure Storage. This is the data that will be processed by each of the tasks that are
            // executed on the compute nodes within the pool.
            List<ResourceFile> inputFiles = new List<ResourceFile>();

            foreach (string filePath in inputFilePaths)
            {
                Console.WriteLine("Uploading file {0} to container [{1}]...", filePath, inputContainerName);

                string blobName = Path.GetFileName(filePath);

                // CloudBlobContainer container = blobClient.GetContainerReference(inputContainerName);
                CloudBlockBlob blobData = container.GetBlockBlobReference(blobName);
                blobData.UploadFromFileAsync(filePath);

                // Set the expiry time and permissions for the blob shared access signature. In this case, no start time is specified,
                // so the shared access signature becomes valid immediately
                SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
                {
                    SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                    Permissions = SharedAccessBlobPermissions.Read
                };

                // Construct the SAS URL for blob
                string sasBlobToken = blobData.GetSharedAccessSignature(sasConstraints);
                string blobSasUri = String.Format("{0}{1}", blobData.Uri, sasBlobToken);

                ResourceFile resourceFile = new ResourceFile(blobSasUri, blobName);

                inputFiles.Add(resourceFile);
            }
            // Get a Batch client using account creds

            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);
            BatchClient batchClient = BatchClient.Open(cred);

            using (batchClient)

            {
                // Create a pool

                Console.WriteLine("Creating pool [{0}]...", PoolId);

                try
                {
                    CloudPool pool = batchClient.PoolOperations.CreatePool(
                    poolId: PoolId,
                    targetDedicatedComputeNodes: 3,
                    virtualMachineSize: "Small",
                    cloudServiceConfiguration: new CloudServiceConfiguration(osFamily: "5")
                    );

                    pool.Commit();
                }
                catch (BatchException be)
                {
                    // Accept the specific error code PoolExists as that is expected if the pool already exists
                    if (be.RequestInformation?.BatchError != null && be.RequestInformation.BatchError.Code == BatchErrorCodeStrings.PoolExists)
                    {
                        Console.WriteLine("The pool {0} already existed when we tried to create it", PoolId);
                    }
                    else
                    {
                        throw; // Any other exception is unexpected
                    }
                }

                // Create a job

                Console.WriteLine("Creating job [{0}]...", JobId);
                
                CloudJob job = batchClient.JobOperations.CreateJob();
                   job.Id = JobId;
                   job.PoolInformation = new PoolInformation { PoolId = PoolId };
                   job.Commit();        
                
                // Create a collection to hold the tasks that we'll be adding to the job

                Console.WriteLine("Adding {0} tasks to job [{1}]...", inputFiles.Count, JobId);

                List<CloudTask> tasks = new List<CloudTask>();

                // Create each of the tasks to process one of the input files. 

                foreach (ResourceFile inputFile in inputFiles)
                {
                    string taskId = "Task" + inputFiles.IndexOf(inputFile);
                    string inputfilename = inputFile.FilePath;
                    string taskCommandLine = String.Format("cmd /c echo 'Processing file {0} in task {1}' & type {0}", inputfilename, taskId);

                    CloudTask task = new CloudTask(taskId, taskCommandLine);
                    task.ResourceFiles = new List<ResourceFile> { inputFile };
                    tasks.Add(task);
                }

                // Add all tasks to the job.
                batchClient.JobOperations.AddTaskAsync(JobId, tasks).Wait();

                               
                // Monitor task success/failure, specifying a maximum amount of time to wait for the tasks to complete
                MonitorTasks(batchClient, JobId, TimeSpan.FromMinutes(30)).Wait();

                // Print task output

                Console.WriteLine();
                Console.WriteLine("Printing task output.");

                IEnumerable<CloudTask> completedtasks = batchClient.JobOperations.ListTasks(JobId);

                foreach (CloudTask task in completedtasks)
                {
                    string nodeId = String.Format(task.ComputeNodeInformation.ComputeNodeId);
                    Console.WriteLine("Task " + task.Id);
                    Console.WriteLine("Node " + nodeId);
                    Console.WriteLine("stdout:" + Environment.NewLine + task.GetNodeFile(Constants.StandardOutFileName).ReadAsString());
                    Console.WriteLine();
                    Console.WriteLine("stderr:" + Environment.NewLine + task.GetNodeFile(Constants.StandardErrorFileName).ReadAsString());
                }
                // Clean up Storage resources
                //DeleteContainerAsync(blobClient, inputContainerName).Wait();

                if (container.DeleteIfExists())
                {
                    Console.WriteLine("Container [{0}] deleted.", inputContainerName);
                }
                else
                {
                    Console.WriteLine("Container [{0}] does not exist, skipping deletion.", inputContainerName);
                }


                // Clean up Batch resources (if the user so chooses)
                Console.WriteLine();
                Console.Write("Delete job? [yes] no: ");
                string response = Console.ReadLine().ToLower();
                if (response != "n" && response != "no")
                {
                    batchClient.JobOperations.DeleteJobAsync(JobId).Wait();
                }

                Console.Write("Delete pool? [yes] no: ");
                response = Console.ReadLine().ToLower();
                if (response != "n" && response != "no")
                {
                    batchClient.PoolOperations.DeletePoolAsync(PoolId).Wait();
                }
            }

            
        }
        // MonitorTasks(): Asynchronously monitors the specified tasks for completion and returns a value indicating
        //   whether all tasks completed successfully within the timeout period.
        //   * batchClient: A BatchClient object.
        //   * jobId: The ID of the job containing the tasks to be monitored.
        //   * timeout: The period of time to wait for the tasks to reach the completed state.
        //   Returns: A Boolean indicating true if all tasks in the specified job completed successfully
        //      (with an exit code of 0) within the specified timeout period; otherwise false.
        private static async Task<bool> MonitorTasks(BatchClient batchClient, string jobId, TimeSpan timeout)
        {
            bool allTasksSuccessful = true;
            const string successMessage = "All tasks reached state Completed.";
            const string failureMessage = "One or more tasks failed to reach the Completed state within the timeout period.";

            // Obtain the collection of tasks currently managed by the job. Note that we use a detail level to
            // specify that only the "id" property of each task should be populated. Using a detail level for
            // all list operations helps to lower response time from the Batch service.
            ODATADetailLevel detail = new ODATADetailLevel(selectClause: "id");
            List<CloudTask> tasks = await batchClient.JobOperations.ListTasks(JobId, detail).ToListAsync();

            Console.WriteLine("Awaiting task completion, timeout in {0}...", timeout.ToString());

            // We use a TaskStateMonitor to monitor the state of our tasks. In this case, we will wait for all tasks to
            // reach the Completed state.
            TaskStateMonitor taskStateMonitor = batchClient.Utilities.CreateTaskStateMonitor();
            try
            {
                await taskStateMonitor.WhenAll(tasks, TaskState.Completed, timeout);
            }
            catch (TimeoutException)
            {
                await batchClient.JobOperations.TerminateJobAsync(jobId, failureMessage);
                Console.WriteLine(failureMessage);
                return false;
            }

            await batchClient.JobOperations.TerminateJobAsync(jobId, successMessage);

            // All tasks have reached the "Completed" state, however, this does not guarantee all tasks completed successfully.
            // Here we further check each task's ExecutionInfo property to ensure that it did not encounter a scheduling error
            // or return a non-zero exit code.

            // Update the detail level to populate only the task id and executionInfo properties.
            // We refresh the tasks below, and need only this information for each task.
            detail.SelectClause = "id, executionInfo";

            foreach (CloudTask task in tasks)
            {
                // Populate the task's properties with the latest info from the Batch service
                await task.RefreshAsync(detail);

                if (task.ExecutionInformation.Result == TaskExecutionResult.Failure)
                {
                    // A task with failure information set indicates there was a problem with the task. It is important to note that
                    // the task's state can be "Completed," yet still have encountered a failure.

                    allTasksSuccessful = false;

                    Console.WriteLine("WARNING: Task [{0}] encountered a failure: {1}", task.Id, task.ExecutionInformation.FailureInformation.Message);
                    if (task.ExecutionInformation.ExitCode != 0)
                    {
                        // A non-zero exit code may indicate that the application executed by the task encountered an error
                        // during execution. As not every application returns non-zero on failure by default (e.g. robocopy),
                        // your implementation of error checking may differ from this example.

                        Console.WriteLine("WARNING: Task [{0}] returned a non-zero exit code - this may indicate task execution or completion failure.", task.Id);
                    }
                }
            }

            if (allTasksSuccessful)
            {
                Console.WriteLine("Success! All tasks completed successfully within the specified timeout period.");
            }

            return allTasksSuccessful;
        }

    }
}