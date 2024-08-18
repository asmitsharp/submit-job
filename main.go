package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/batch"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("Usage: %s <number1> <number2>", os.Args[0])
	}

	num1, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid Number1 : %v", err)
	}

	num2, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("Invalid Number1 : %v", err)
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-north-1"),
	})

	if err != nil {
		log.Fatalf("Failed to create session : %v", err)
	}

	batchSvc := batch.New(sess)
	cloudwatchSvc := cloudwatchlogs.New(sess)

	jobName := fmt.Sprintf("addition-job-%d-%d", num1, num2)
	jobQueue := "addition-job-queue"
	jobDefinition := "arn:aws:batch:eu-north-1:533267319584:job-definition/addition-job-definition:2"

	input := &batch.SubmitJobInput{
		JobName:       aws.String(jobName),
		JobQueue:      aws.String(jobQueue),
		JobDefinition: aws.String(jobDefinition),
		ContainerOverrides: &batch.ContainerOverrides{
			Environment: []*batch.KeyValuePair{
				{
					Name:  aws.String("NUMBER1"),
					Value: aws.String(strconv.Itoa(num1)),
				},
				{
					Name:  aws.String("NUMBER2"),
					Value: aws.String(strconv.Itoa(num2)),
				},
			},
		},
	}

	result, err := batchSvc.SubmitJob(input)
	if err != nil {
		log.Fatalf("Error submitting job: %v", err)
	}

	fmt.Printf("Successfully submitted job %s with ID %s\n", jobName, *result.JobId)

	// waiting for job completion
	fmt.Println("Waiting for job to complete...")
	err = waitForJobCompletion(batchSvc, result.JobId)
	if err != nil {
		log.Fatalf("Error waiting for job completion: %v", err)
	}

	// retrieving the job logs for output
	fmt.Println("Job completed. Retrieving logs...")
	err = retrieveJobLogs(cloudwatchSvc, result.JobId, jobDefinition)
	if err != nil {
		log.Fatalf("Error retrieving job logs: %v", err)
	}
}

func waitForJobCompletion(svc *batch.Batch, jobId *string) error {
	for {
		input := &batch.DescribeJobsInput{
			Jobs: []*string{jobId},
		}

		result, err := svc.DescribeJobs(input)
		if err != nil {
			return err
		}

		if len(result.Jobs) == 0 {
			return fmt.Errorf("JOB NOT FOUND")
		}

		status := *result.Jobs[0].Status
		if status == "SUCCEEDED" || status == "FAILED" {
			fmt.Printf("Job %s finished with status : %s\n", *jobId, status)
			return nil
		}

		fmt.Printf("Job status : %s, Waiting...\n", status)
		time.Sleep(10 * time.Second)
	}
}
func retrieveJobLogs(svc *cloudwatchlogs.CloudWatchLogs, jobId *string, jobDefinition string) error {
	logStreams, err := svc.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        aws.String("/aws/batch/job"),
		LogStreamNamePrefix: aws.String("addition-job-definition/default/"),
	})
	if err != nil {
		return err
	}

	// Find the most recent log stream that matches the job definition name
	var mostRecentLogStream *cloudwatchlogs.LogStream
	for _, stream := range logStreams.LogStreams {
		if strings.HasPrefix(*stream.LogStreamName, "addition-job-definition/default/") {
			if mostRecentLogStream == nil || (stream.LastEventTimestamp != nil && (mostRecentLogStream.LastEventTimestamp == nil || *stream.LastEventTimestamp > *mostRecentLogStream.LastEventTimestamp)) {
				mostRecentLogStream = stream
			}
		}
	}

	if mostRecentLogStream == nil {
		return fmt.Errorf("log stream not found for job %s", *jobId)
	}

	// Now that we have the correct log stream name, retrieve the log events
	params := &cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  aws.String("/aws/batch/job"),
		LogStreamName: mostRecentLogStream.LogStreamName,
		StartFromHead: aws.Bool(true),
		Limit:         aws.Int64(10), // Limit the number of log events retrieved
	}

	resp, err := svc.GetLogEvents(params)
	if err != nil {
		return err
	}

	fmt.Println("Job Logs:")
	for _, event := range resp.Events {
		fmt.Println(*event.Message)
	}

	return nil
}
