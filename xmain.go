import (
    "context"
    "fmt"
    "log"
    "os"
    "text/tabwriter"

    "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/runtime/schema"
    "k8s.io/client-go/dynamic"
    "k8s.io/client-go/tools/clientcmd"
)

// Replace the existing code with the following

// Create a dynamic client
config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
if err != nil {
    log.Fatalf("Error building kubeconfig: %s", err)
}

dynamicClient, err := dynamic.NewForConfig(config)
if err != nil {
    log.Fatalf("Error creating dynamic client: %s", err)
}

// Define the GVR (GroupVersionResource) for the CRD
gvr := schema.GroupVersionResource{
    Group:    "cloud.crate.io", // Replace with your CRD's group
    Version:  "v1",             // Replace with your CRD's version
    Resource: "cratedbs",       // Replace with your CRD's resource name
}

// List the CRDs from all namespaces
cratedbs, err := dynamicClient.Resource(gvr).Namespace(v1.NamespaceAll).List(context.TODO(), v1.ListOptions{})
if err != nil {
    log.Fatalf("Error listing cratedbs: %s", err)
}

fmt.Printf("There are %d cratedbs in the cluster\n", len(cratedbs.Items))

// Print the CRDs in a nicely formatted table
writer := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
fmt.Fprintln(writer, "NAME\tNAMESPACE\tSTATUS")

for _, cratedb := range cratedbs.Items {
    name := cratedb.GetName()
    namespace := cratedb.GetNamespace()
    status := cratedb.Object["status"].(map[string]interface{})["phase"].(string) // Adjust based on your CRD's status structure

    fmt.Fprintf(writer, "%s\t%s\t%s\n", name, namespace, status)
}

writer.Flush()
