package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/ONSdigital/dp-api-clients-go/v2/zebedee"
	dpEs "github.com/ONSdigital/dp-elasticsearch/v3"
	dpEsClient "github.com/ONSdigital/dp-elasticsearch/v3/client"
	"github.com/ONSdigital/dp-net/v2/awsauth"
	dphttp2 "github.com/ONSdigital/dp-net/v2/http"
	"github.com/ONSdigital/dp-search-api/elasticsearch"
	extractorModels "github.com/ONSdigital/dp-search-data-extractor/models"
	importerModels "github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/transform"
)

var (
	maxConcurrentExtractions = 20
	maxConcurrentIndexings   = 20
)

type cliConfig struct {
	aws          AWSConfig
	zebedeeURL   string
	esURL        string
	signRequests bool
}

type AWSConfig struct {
	filename              string
	profile               string
	region                string
	service               string
	tlsInsecureSkipVerify bool
}

type zebedeeClient interface {
	GetPublishedIndex(ctx context.Context, piRequest *zebedee.PublishedIndexRequestParams) (zebedee.PublishedIndex, error)
	GetPublishedData(ctx context.Context, uriString string) ([]byte, error)
}

type Document struct {
	URI  string
	Body []byte
}

func main() {
	fmt.Printf("Hola %s!\n", Name)

	ctx := context.Background()
	cfg := getConfig(ctx)

	hcClienter := dphttp2.NewClient()
	hcClienter.SetMaxRetries(2)
	hcClienter.SetTimeout(30 * time.Second) // Published Index takes about 10s to return so add a bit more
	zebClient := zebedee.NewClientWithClienter(cfg.zebedeeURL, hcClienter)

	esHttpClient := hcClienter
	if cfg.signRequests {
		fmt.Println("Use a signing roundtripper client")
		awsSignerRT, err := awsauth.NewAWSSignerRoundTripper(cfg.aws.filename, cfg.aws.filename, cfg.aws.region, cfg.aws.service,
			awsauth.Options{TlsInsecureSkipVerify: cfg.aws.tlsInsecureSkipVerify})
		if err != nil {
			log.Fatal(ctx, "Failed to create http signer", err)
		}

		esHttpClient = dphttp2.NewClientWithTransport(awsSignerRT)
	}

	dpEsClient, err := dpEs.NewClient(dpEsClient.Config{
		ClientLib: dpEsClient.GoElasticV710,
		Address:   cfg.esURL,
		Transport: esHttpClient,
	})
	if err != nil {
		log.Fatal(ctx, "Failed to create dp-elasticsearch client", err)
	}

	urisChan := uriProducer(ctx, zebClient, 1000)
	//urisChan := fakeUriProducer()
	extractedChan, extractionFailuresChan := docExtractor(ctx, zebClient, urisChan, maxConcurrentExtractions)
	transformedChan := docTransformer(extractedChan)
	indexedChan := docIndexer(ctx, dpEsClient, transformedChan, maxConcurrentIndexings)

	summarize(indexedChan, extractionFailuresChan)

	if promptUserToCleanIndices() {
		cleanOldIndices(ctx, dpEsClient)
	}
}

func uriProducer(ctx context.Context, z zebedeeClient, limit int) chan string {
	uriChan := make(chan string)
	go func() {
		defer close(uriChan)
		items := getPublishedURIs(ctx, z)
		if len(items) > limit {
			items = items[:limit]
		}
		for _, item := range items {
			uriChan <- item.URI
		}
		fmt.Println("Finished listing uris")
	}()
	return uriChan
}

// func fakeUriProducer() chan string {
// 	uriChan := make(chan string)
// 	go func() {
// 		defer close(uriChan)

// 		urisToIndex := []string{
// 			"/peoplepopulationandcommunity/housing/articles/housepricestatisticsforsmallareasinenglandandwales/2015-02-17",
// 		}

// 		for _, uri := range urisToIndex {
// 			for i := 0; i < 1; i++ {
// 				uriChan <- uri
// 			}
// 		}
// 		fmt.Println("Finished listing uris")
// 	}()
// 	return uriChan
// }

func getPublishedURIs(ctx context.Context, z zebedeeClient) []zebedee.PublishedIndexItem {
	index, err := z.GetPublishedIndex(ctx, &zebedee.PublishedIndexRequestParams{})
	if err != nil {
		//TODO error handling
		log.Fatalf("Fatal error getting index from zebedee: %s", err)
	}
	fmt.Printf("Fetched %d uris from zebedee\n", index.Count)
	return index.Items
}

func docExtractor(ctx context.Context, z zebedeeClient, uriChan chan string, maxExtractions int) (chan Document, chan string) {
	extractedChan := make(chan Document)
	extractionFailuresChan := make(chan string)
	go func() {
		defer close(extractedChan)
		defer close(extractionFailuresChan)

		var wg sync.WaitGroup

		for w := 0; w < maxExtractions; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				extractDoc(ctx, z, uriChan, extractedChan, extractionFailuresChan)
			}()
		}
		wg.Wait()
		fmt.Println("Finished extracting docs")
	}()
	return extractedChan, extractionFailuresChan
}

func extractDoc(ctx context.Context, z zebedeeClient, uriChan chan string, extractedChan chan Document, extractionFailuresChan chan string) {
	for uri := range uriChan {
		body, err := z.GetPublishedData(ctx, uri)
		//time.Sleep(time.Second)
		if err != nil {
			extractionFailuresChan <- uri
		}

		extractedDoc := Document{
			URI:  uri,
			Body: body,
		}
		extractedChan <- extractedDoc
	}
}

func docTransformer(extractedChan chan Document) chan Document {
	transformedChan := make(chan Document)
	go func() {
		defer close(transformedChan)
		var wg sync.WaitGroup

		for extractedDoc := range extractedChan {
			wg.Add(1)
			go func(doc Document) {
				defer wg.Done()
				transformDoc(doc, transformedChan)
			}(extractedDoc)
		}
		wg.Wait()
		fmt.Println("Finished transforming docs")
	}()
	return transformedChan
}

func transformDoc(extractedDoc Document, transformedChan chan Document) {

	//byte slice to Json & unMarshall Json
	var zebedeeData extractorModels.ZebedeeData
	err := json.Unmarshal(extractedDoc.Body, &zebedeeData)
	if err != nil {
		log.Fatal("error while attempting to unmarshal zebedee response into zebedeeData", err) //TODO proper error handling
	}

	exporterEventData := extractorModels.MapZebedeeDataToSearchDataImport(zebedeeData, -1)
	importerEventData := importerModels.SearchDataImportModel(exporterEventData)
	esModel := transform.NewTransformer().TransformEventModelToEsModel(&importerEventData)

	body, err := json.Marshal(esModel)
	if err != nil {
		log.Fatal("error marshal to json", err) //TODO error handling
		return
	}

	transformedDoc := Document{
		URI:  extractedDoc.URI,
		Body: body,
	}
	transformedChan <- transformedDoc
}

func docIndexer(ctx context.Context, dpEsClient dpEsClient.Client, transformedChan chan Document, maxIndexings int) chan bool {
	indexedChan := make(chan bool)
	go func() {
		defer close(indexedChan)

		indexName := createIndexName("ons")

		err := dpEsClient.CreateIndex(ctx, indexName, elasticsearch.GetSearchIndexSettings())
		if err != nil {
			log.Fatal(ctx, "error creating index", err)
		}

		fmt.Printf("Index created: %s\n", indexName)

		var wg sync.WaitGroup

		for w := 0; w < maxIndexings; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				indexDoc(ctx, dpEsClient, transformedChan, indexedChan, indexName)
			}()
		}
		wg.Wait()
		fmt.Println("Finished indexing docs")

		swapAliases(ctx, dpEsClient, indexName)
	}()
	return indexedChan
}

func createIndexName(s string) string {
	now := time.Now()
	return fmt.Sprintf("%s%d", s, now.UnixMicro())
}

func indexDoc(ctx context.Context, dpEsClient dpEsClient.Client, transformedChan chan Document, indexedChan chan bool, indexName string) {
	for transformedDoc := range transformedChan {

		id := url.PathEscape(transformedDoc.URI) //TODO this isn't right, the client should url-escape the id
		indexed := true

		err := dpEsClient.AddDocument(ctx, indexName, id, transformedDoc.Body, nil)

		if err != nil {
			indexed = false
		}

		indexedChan <- indexed
	}
}

func swapAliases(ctx context.Context, dpEsClient dpEsClient.Client, indexName string) {
	err := dpEsClient.UpdateAliases(ctx, "ons", []string{"ons*"}, []string{indexName})
	if err != nil {
		log.Fatalf("error swapping aliases: %v", err)
	}
}

func summarize(indexedChan chan bool, extractionFailuresChan chan string) {
	totalIndexed, totalFailed := 0, 0
	for range extractionFailuresChan {
		totalFailed++
	}
	for indexed := range indexedChan {
		if indexed {
			totalIndexed++
		} else {
			totalFailed++
		}
	}

	fmt.Printf("Indexed: %d, Failed: %d\n", totalIndexed, totalFailed)
}

func promptUserToCleanIndices() bool {
	//TODO prompt

	return true
}

type aliasResponse map[string]indexDetails

type indexDetails struct {
	Aliases map[string]interface{} `json:"aliases"`
}

func cleanOldIndices(ctx context.Context, dpEsClient dpEsClient.Client) {
	body, err := dpEsClient.GetAlias(ctx) // Create this method via dp-elasticsearch v3 lib
	if err != nil {
		log.Fatalf("Error: Indices.GetAlias: %s", err)
	}

	//fmt.Printf("GetAliasResponse:%v\n", res)
	var r aliasResponse
	if err := json.Unmarshal(body, &r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}

	toDelete := []string{}
	for index, details := range r {
		if strings.HasPrefix(index, "ons") && !doesIndexHaveAlias(details, "ons") {
			toDelete = append(toDelete, index)
		}
	}

	deleteIndicies(ctx, dpEsClient, toDelete)
}

func doesIndexHaveAlias(details indexDetails, alias string) bool {
	for k, _ := range details.Aliases {
		if k == alias {
			return true
		}
	}
	return false
}

func deleteIndicies(ctx context.Context, dpEsClient dpEsClient.Client, indicies []string) {

	if err := dpEsClient.DeleteIndices(ctx, indicies); err != nil {
		log.Fatalf("Error: Indices.GetAlias: %s", err)
	}

	fmt.Printf("Deleted Indicies: %s\n", strings.Join(indicies, ","))
}
