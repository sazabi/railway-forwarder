package railway

import (
	"context"
	"fmt"
)

const environmentQuery = `query forwarderEnvironment($id: String!) {
  environment(id: $id) {
    id
    projectId
    deployments {
      edges { node { id serviceId status } }
    }
    serviceInstances {
      edges {
        node {
          serviceId
          domains {
            serviceDomains { domain }
            customDomains { domain }
          }
        }
      }
    }
  }
}`

const projectQuery = `query forwarderProject($id: String!) {
  project(id: $id) {
    id
    name
    environments { edges { node { id name } } }
    services { edges { node { id name } } }
  }
}`

// Environment is a point-in-time view of the Railway environment the forwarder runs in.
type Environment struct {
	ID          string
	Name        string
	ProjectID   string
	ProjectName string
	// Services maps service ID to service name.
	Services map[string]string
	// HTTPDeployments maps the ID of every successful deployment of a service with a
	// public domain to its service ID. Only these deployments produce HTTP logs.
	HTTPDeployments map[string]string
}

type environmentData struct {
	Environment struct {
		ID          string `json:"id"`
		ProjectID   string `json:"projectId"`
		Deployments struct {
			Edges []struct {
				Node struct {
					ID        string `json:"id"`
					ServiceID string `json:"serviceId"`
					Status    string `json:"status"`
				} `json:"node"`
			} `json:"edges"`
		} `json:"deployments"`
		ServiceInstances struct {
			Edges []struct {
				Node struct {
					ServiceID string `json:"serviceId"`
					Domains   struct {
						ServiceDomains []struct {
							Domain string `json:"domain"`
						} `json:"serviceDomains"`
						CustomDomains []struct {
							Domain string `json:"domain"`
						} `json:"customDomains"`
					} `json:"domains"`
				} `json:"node"`
			} `json:"edges"`
		} `json:"serviceInstances"`
	} `json:"environment"`
}

type projectData struct {
	Project struct {
		ID           string `json:"id"`
		Name         string `json:"name"`
		Environments struct {
			Edges []struct {
				Node struct {
					ID   string `json:"id"`
					Name string `json:"name"`
				} `json:"node"`
			} `json:"edges"`
		} `json:"environments"`
		Services struct {
			Edges []struct {
				Node struct {
					ID   string `json:"id"`
					Name string `json:"name"`
				} `json:"node"`
			} `json:"edges"`
		} `json:"services"`
	} `json:"project"`
}

// FetchEnvironment loads the environment's project, services, and HTTP-serving deployments.
func (c *Client) FetchEnvironment(ctx context.Context, environmentID string) (*Environment, error) {
	var env environmentData
	if err := c.Query(ctx, environmentQuery, map[string]any{"id": environmentID}, &env); err != nil {
		return nil, fmt.Errorf("fetch environment %s: %w", environmentID, err)
	}
	if env.Environment.ProjectID == "" {
		return nil, fmt.Errorf("fetch environment %s: not found or not visible to this token", environmentID)
	}
	var proj projectData
	if err := c.Query(ctx, projectQuery, map[string]any{"id": env.Environment.ProjectID}, &proj); err != nil {
		return nil, fmt.Errorf("fetch project %s: %w", env.Environment.ProjectID, err)
	}
	return buildEnvironment(environmentID, &env, &proj), nil
}

func buildEnvironment(environmentID string, env *environmentData, proj *projectData) *Environment {
	out := &Environment{
		ID:              environmentID,
		ProjectID:       env.Environment.ProjectID,
		ProjectName:     proj.Project.Name,
		Services:        make(map[string]string),
		HTTPDeployments: make(map[string]string),
	}
	for _, e := range proj.Project.Environments.Edges {
		if e.Node.ID == environmentID {
			out.Name = e.Node.Name
		}
	}
	for _, e := range proj.Project.Services.Edges {
		out.Services[e.Node.ID] = e.Node.Name
	}
	withDomain := make(map[string]bool)
	for _, e := range env.Environment.ServiceInstances.Edges {
		d := e.Node.Domains
		if len(d.ServiceDomains) > 0 || len(d.CustomDomains) > 0 {
			withDomain[e.Node.ServiceID] = true
		}
	}
	for _, e := range env.Environment.Deployments.Edges {
		n := e.Node
		if n.Status == "SUCCESS" && withDomain[n.ServiceID] {
			out.HTTPDeployments[n.ID] = n.ServiceID
		}
	}
	return out
}
