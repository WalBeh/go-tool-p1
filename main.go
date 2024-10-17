package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

// GetKubeConfig tries to load the kubeconfig file from the KUBECONFIG environment variable or the default location.
func GetKubeConfig(desiredContext string) (*rest.Config, error) {
	var kubeconfigPaths []string
	if envKubeConfig := os.Getenv("KUBECONFIG"); envKubeConfig != "" {
		// Split the KUBECONFIG paths by colon for Linux/macOS or semicolon for Windows
		kubeconfigPaths = filepath.SplitList(envKubeConfig)
	} else {
		// Fallback to the default kubeconfig path
		kubeconfigPaths = []string{filepath.Join(homedir.HomeDir(), ".kube", "config")}
	}

	// Load and merge the kubeconfig files
	loadingRules := &clientcmd.ClientConfigLoadingRules{Precedence: kubeconfigPaths}
	configOverrides := &clientcmd.ConfigOverrides{}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)

	// Get the raw config to set the desired context
	rawConfig, err := kubeConfig.RawConfig()
	if err != nil {
		return nil, fmt.Errorf("error loading raw kubeconfig: %w", err)
	}
	rawConfig.CurrentContext = desiredContext

	// Create the client config
	config, err := clientcmd.NewDefaultClientConfig(rawConfig, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("error creating client config: %w", err)
	}

	return config, nil
}

// listStatefulSetsInNamespace lists the StatefulSets in the given namespace and returns their names and replicas
func listStatefulSetsInNamespace(clientset *kubernetes.Clientset, namespace string) (map[string]int32, error) {
	statefulSets, err := clientset.AppsV1().StatefulSets(namespace).List(context.TODO(), v1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("error listing statefulsets in namespace %s: %w", namespace, err)
	}

	statefulSetInfo := make(map[string]int32)
	for _, statefulSet := range statefulSets.Items {
		statefulSetInfo[statefulSet.GetName()] = *statefulSet.Spec.Replicas
	}

	return statefulSetInfo, nil
}

// waitForGreenHealth waits until the cratedb health status is GREEN
func waitForGreenHealth(dynamicClient dynamic.Interface, namespace, cratedbName string) error {
	// Define the GVR (GroupVersionResource) for the CRD
	gvr := schema.GroupVersionResource{
		Group:    "cloud.crate.io", // Replace with your CRD's group
		Version:  "v1",             // Replace with your CRD's version
		Resource: "cratedbs",       // Replace with your CRD's resource name
	}

	for {
		// Get the cratedb resource to check the health status
		cratedb, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(context.TODO(), cratedbName, v1.GetOptions{})
		if err != nil {
			return fmt.Errorf("error getting cratedb %s: %w", cratedbName, err)
		}

		// Check the health status
		health := "unknown"
		if status, found, _ := unstructured.NestedMap(cratedb.Object, "status", "crateDBStatus"); found {
			if healthValue, found, _ := unstructured.NestedString(status, "health"); found {
				health = healthValue
			}
		}

		if health == "GREEN" {
			log.Printf("    GREEN status for %s", cratedbName)
			return nil
		}

		log.Printf("%s unhealthy", cratedbName)
		time.Sleep(10 * time.Second) // Wait for 10 seconds before checking again
	}
}

// deletePodsInStatefulSet deletes all pods in the given StatefulSet
func rollingRestart(clientset *kubernetes.Clientset, dynamicClient dynamic.Interface, namespace string, statefulSetName string) error {
	pods, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), v1.ListOptions{})
	if err != nil {
		log.Fatalf("Error listing pods: %s", err)
	}
	if err != nil {
		log.Fatalf("Error listing pods: %s", err)
	}

	for _, pod := range pods.Items {
		if !strings.HasPrefix(pod.GetName(), statefulSetName) {
			continue
		}
		log.Printf("  POD:  %s", pod.GetName())

		// this happens to be the cratedb name - a bit shaky
		cratedbName := strings.Trim(statefulSetName, "crate-data-hot-")
		log.Printf("  >>>>>pre check")
		err := waitForGreenHealth(dynamicClient, namespace, cratedbName)
		if err != nil {
			return err
		}
		//err := clientset.CoreV1().Pods(namespace).Delete(context.TODO(), pod.GetName(), v1.DeleteOptions{})
		err = nil
		if err != nil {
			return fmt.Errorf("error deleting pod %s: %w", pod.GetName(), err)
		}
		log.Printf("  <<<<<<post check")
		err = waitForGreenHealth(dynamicClient, namespace, cratedbName)
		if err != nil {
			return err
		}

		log.Printf("  DELETED Pod: %s", pod.GetName())
	}

	return nil
}

func main() {
	// Set kubecontext flag
	desiredContext := flag.String("ctx", "aks1-eastus-dev", "The Kubernetes context to use")
	flag.Parse()
	config, err := GetKubeConfig(*desiredContext)
	if err != nil {
		log.Fatalf("Error obtaining kubeconfig: %s", err)
	}
	log.Printf("Using context %q", *desiredContext)

	// Create a dynamic client -- needed for the CRDs
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error creating dynamic client: %s", err)
	}

	// Create a typed clientset - needed for sts, pods, etc.
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error creating clientset: %s", err)
	}

	// Define the GVR (GroupVersionResource) for the cratedbs
	gvr := schema.GroupVersionResource{
		Group:    "cloud.crate.io",
		Version:  "v1",
		Resource: "cratedbs",
	}

	cratedbs, err := dynamicClient.Resource(gvr).Namespace(v1.NamespaceAll).List(context.TODO(), v1.ListOptions{})
	if err != nil {
		log.Fatalf("Error listing cratedbs: %s", err)
	}

	fmt.Printf("There are %d cratedbs in the cluster\n", len(cratedbs.Items))

	// Print the CRDs in a nicely formatted table
	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "NAMESPACE\tNAME\tCLUSTERNAME\tHEALTH")

	for _, cratedb := range cratedbs.Items {
		//name := cratedb.GetName()
		namespace := cratedb.GetNamespace()
		health := "unknown"
		clustername := "unknown"

		// Access the status.crateDBStatus.health field
		if status, found, _ := unstructured.NestedMap(cratedb.Object, "status", "crateDBStatus"); found {
			if healthValue, found, _ := unstructured.NestedString(status, "health"); found {
				health = healthValue
			}
		}

		// Access the spec.cluster.name field
		if spec, found, _ := unstructured.NestedMap(cratedb.Object, "spec", "cluster"); found {
			if clusternameValue, found, _ := unstructured.NestedString(spec, "name"); found {
				clustername = clusternameValue
			}
		}

		// List the statefulsets in the namespace
		//      there should be one statefulset per cratedb
		statefulSetInfo, err := listStatefulSetsInNamespace(clientset, namespace)
		if err != nil {
			log.Printf("Error listing statefulsets in namespace %s: %s", namespace, err)
		}
		for statefulSetName, replicas := range statefulSetInfo {
			//fmt.Fprintf(writer, "%s\t%s\t%s\t%s\t%s (%d replicas)\n", namespace, name, clustername, health, statefulSetName, replicas)
			log.Printf("----------------------------------")
			log.Printf(">>>>----- %s (replicas=%d) %s", clustername, replicas, health)
			//log.Printf("%s\t%s\t%s\t%s\t%s (%d replicas)\n", namespace, name, clustername, health, statefulSetName, replicas)

			// Do a rolling restart of the statefulset
			err := rollingRestart(clientset, dynamicClient, namespace, statefulSetName)
			if err != nil {
				log.Printf("Error deleting pods in statefulset %s: %s", statefulSetName, err)
			}
		}

		//fmt.Fprintf(writer, "%s\t%s\t%s\t%s\n", namespace, name, clustername, health)
	}

	//writer.Flush()
}
