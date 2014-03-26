package com.faceture.glisten

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials

import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflow
import com.amazonaws.services.simpleworkflow.AmazonSimpleWorkflowClient
import com.amazonaws.services.simpleworkflow.flow.ActivityWorker
import com.amazonaws.services.simpleworkflow.flow.WorkflowWorker
import com.amazonaws.services.simpleworkflow.model.DescribeWorkflowExecutionRequest
import com.amazonaws.services.simpleworkflow.model.DomainInfo
import com.amazonaws.services.simpleworkflow.model.ListDomainsRequest
import com.amazonaws.services.simpleworkflow.model.RegisterDomainRequest
import com.amazonaws.services.simpleworkflow.model.RegistrationStatus
import com.netflix.glisten.InterfaceBasedWorkflowClient
import com.netflix.glisten.WorkflowClientFactory
import com.netflix.glisten.WorkflowDescriptionTemplate
import com.netflix.glisten.WorkflowTags
import com.netflix.glisten.example.trip.BayAreaLocation
import com.netflix.glisten.example.trip.BayAreaTripActivitiesImpl
import com.netflix.glisten.example.trip.BayAreaTripWorkflow
import com.netflix.glisten.example.trip.BayAreaTripWorkflowDescriptionTemplate
import com.netflix.glisten.example.trip.BayAreaTripWorkflowImpl
import org.apache.commons.lang3.BooleanUtils
import org.apache.log4j.Logger

/**
 * Example for how to use the Netflix Glisten library.
 * https://github.com/Netflix/glisten
 */
class GlistenExample {

    // AWS constants
    private static final AwsCredentialsPath = System.getProperty("user.home") + "/" + "AwsCredentials.properties"

    // SWF constants
    private static final DomainName = "GlistenExample"
    private static final DomainDescription = "Domain for running the GlistenExample program."
    private static final String TaskList = "GlistenExample"

    // logger
    private static final log = Logger.getLogger(GlistenExample)

    /**
     * Main entry point to the application
     * @param args -- program arguments (currently unused)
     */
    static void main(String[] args) {

        log.info("Starting...")

        // load the AWS credentials
        final Properties properties = new Properties();
        properties.load(new FileInputStream(AwsCredentialsPath));
        final accessKey = properties.getProperty("accessKey")
        final secretKey = properties.getProperty("secretKey")
        final AWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey)

        log.info("Loaded AWS credentials.")

        // create the AWS SWF client
        final AmazonSimpleWorkflow simpleWorkflow = new AmazonSimpleWorkflowClient(awsCredentials)

        log.info("Created SWF client.")

        // Make sure the domain is registered
        final listDomainsRequest = new ListDomainsRequest().withRegistrationStatus(RegistrationStatus.REGISTERED)
        final domainInfos = simpleWorkflow.listDomains(listDomainsRequest)
        final domainExists =
                domainInfos.getDomainInfos().find{ DomainInfo domainInfo ->
                    domainInfo.getName() == DomainName
                } != null

        if (!domainExists) {
            // we need to register the domain because it doesn't exist
            final registerDomainRequest = new RegisterDomainRequest()
                .withName(DomainName)
                .withDescription(DomainDescription)
                .withWorkflowExecutionRetentionPeriodInDays("1")

            simpleWorkflow.registerDomain(registerDomainRequest)

            log.info("Registered SWF domain $DomainName.")
        }



        ////////////////////////////
        // get input from the user
        ////////////////////////////

        log.info("Getting user info...")

        // What's his or her name?
        println("What's your name?")
        Scanner s = new Scanner(System.in)
        final userName = s.next()

        // Where has he or she visited?
        final List<BayAreaLocation> previouslyVisited = []
        for (BayAreaLocation location: BayAreaLocation.values()) {
            println("Have you visited $location?")
            final hasVisited = BooleanUtils.toBooleanObject(s.next())

            if (hasVisited) {
                previouslyVisited.add(location)
            }
        }


        ////////////////////////
        // Setup the workflow
        ////////////////////////

        log.info("Creating workflow objects...")

        // create the Glisten WorkflowClientFactory
        final workflowClientFactory = new WorkflowClientFactory(simpleWorkflow, DomainName, TaskList)

        // the description template
        final WorkflowDescriptionTemplate workflowDescriptionTemplate = new BayAreaTripWorkflowDescriptionTemplate()

        // create tags -- these are required per https://github.com/Netflix/glisten/issues/21
        final workflowTags = new WorkflowTags("BayAreaTripWorkflowTags")

        // create the client for the BayAreaTripWorkflow
        final InterfaceBasedWorkflowClient<BayAreaTripWorkflow> glistenWorkflowClient =
                workflowClientFactory.getNewWorkflowClient(BayAreaTripWorkflow, workflowDescriptionTemplate, workflowTags)

        // create and start the workflow worker
        final workflowWorker = new WorkflowWorker(simpleWorkflow, DomainName, TaskList)
        workflowWorker.setWorkflowImplementationTypes([BayAreaTripWorkflowImpl])
        workflowWorker.start()

        // create the activity object
        BayAreaTripActivitiesImpl bayAreaTripActivities = new BayAreaTripActivitiesImpl()

        // create and start the activity worker
        final activityWorker = new ActivityWorker(simpleWorkflow, DomainName, TaskList)
        activityWorker.addActivitiesImplementations([bayAreaTripActivities])
        activityWorker.start()

        //////////////////////
        // start the workflow
        //////////////////////
        glistenWorkflowClient.asWorkflow().start(userName, previouslyVisited)

        final workflowExecution = glistenWorkflowClient.getWorkflowExecution()
        final workflowId = workflowExecution.getWorkflowId()

        log.info("Running workflow execution $workflowId")

        def running = true

        ///////////////////////////
        // wait for it to finish
        ///////////////////////////
        while(running) {
            log.info("Workflow still running...")

            Thread.currentThread().sleep(5 * 1000)

            final describeWorkflowExecutionRequest = new DescribeWorkflowExecutionRequest()
                    .withExecution(workflowExecution)
                    .withDomain(DomainName)

            final workflowExecutionDetail = simpleWorkflow.describeWorkflowExecution(describeWorkflowExecutionRequest)

            final workflowExecutionInfo = workflowExecutionDetail.getExecutionInfo()
            final executionStatus = workflowExecutionInfo.getExecutionStatus()

            running = executionStatus == "OPEN"
        }

        // This would be much better if it checked to see if the workflow were completed.
        // Unfortunately, I don't know how to reliably get the Workflow Execution ID for the workflow we just started.

        // TODO: programmatically retrieve events and activities and print to console
        // TODO: gracefully shutdown all workers

        log.info("The workflow is now complete.")

    }


}
