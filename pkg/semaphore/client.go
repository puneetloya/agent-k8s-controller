package semaphore

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/renderedtext/agent-k8s-stack/pkg/agenttypes"
	"k8s.io/klog/v2"
)

// A request that hangs would stop the controller from creating any job.
var httpClient = &http.Client{Timeout: 30 * time.Second}

type Client struct {
	Endpoint string
}

func NewClient(endpoint string) *Client {
	return &Client{
		Endpoint: endpoint,
	}
}

type Response struct {
	Jobs []Job `json:"jobs" yaml:"jobs"`
}

// There are more fields here, but I only care about the ID for now.
type Job struct {
	ID string `json:"id" yaml:"id"`
}

type JobRequest struct {
	JobID       string
	MachineType string
}

func (a *Client) ListJobs(agentTypes []*agenttypes.AgentType) ([]JobRequest, error) {
	jobRequests := []JobRequest{}

	for _, agentType := range agentTypes {
		jobs, err := a.listJobsForAgentType(agentType)
		if err != nil {
			klog.ErrorS(err, "error listing jobs for agent type", "agentType", agentType.AgentTypeName)
			continue
		}

		for _, j := range jobs {
			jobRequest := JobRequest{
				JobID:       j,
				MachineType: agentType.AgentTypeName,
			}
			jobRequests = append(jobRequests, jobRequest)
		}
	}

	return jobRequests, nil
}

func (a *Client) listJobsForAgentType(agentType *agenttypes.AgentType) ([]string, error) {
	URL := fmt.Sprintf("https://%s/api/v1/self_hosted_agents/jobs", a.Endpoint)
	req, err := http.NewRequest(http.MethodGet, URL, nil)
	if err != nil {
		return []string{}, err
	}

	req.Header.Add("Authorization", fmt.Sprintf("Token %s", agentType.RegistrationToken))
	response, err := httpClient.Do(req)
	if err != nil {
		return []string{}, err
	}

	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return []string{}, err
	}

	if response.StatusCode != http.StatusOK {
		return []string{}, fmt.Errorf("unexpected status %d", response.StatusCode)
	}

	apiResponse := Response{}
	err = json.Unmarshal(body, &apiResponse)
	if err != nil {
		return []string{}, err
	}

	jobs := []string{}
	for _, j := range apiResponse.Jobs {
		jobs = append(jobs, j.ID)
	}

	return jobs, nil
}

func in(list []string, item string) bool {
	for _, i := range list {
		if i == item {
			return true
		}
	}

	return false
}
