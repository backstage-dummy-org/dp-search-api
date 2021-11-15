package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ONSdigital/dp-api-clients-go/v2/zebedee"
	dpelasticsearch "github.com/ONSdigital/dp-elasticsearch/v2/elasticsearch"
	"github.com/ONSdigital/dp-search-api/config"
	"github.com/ONSdigital/dp-search-api/elasticsearch"
	exporterModels "github.com/ONSdigital/dp-search-data-extractor/models"
	importerModels "github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/transform"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

const zebedeeURI = "http://localhost:8082"

var (
	maxConcurrentExtractions = 20
	maxConcurrentIndexings   = 20
)

type zebedeeClient interface {
	GetPublishedIndex(ctx context.Context) (zebedee.PublishedIndex, error)
	GetPublishedData(ctx context.Context, uriString string) ([]byte, error)
}

type elasticSearchClient interface {
	CreateIndex(ctx context.Context, indexName string, indexSettings []byte) (int, error)
	AddDocument(ctx context.Context, indexName, documentType, documentID string, document []byte) (int, error)
}

type Document struct {
	URI  string
	Body []byte
}

//type TransformedDoc struct {
//	ID  string
//	Body []byte
//}

func main() {
	ctx := context.Background()
	cfg, err := config.Get()
	if err != nil {
		log.Fatal(ctx, "error retrieving config", err)
		os.Exit(1)
	}

	zebClient := zebedee.New(zebedeeURI)
	esClient := dpelasticsearch.NewClient(cfg.ElasticSearchAPIURL, cfg.SignElasticsearchRequests, 5)

	urisChan := uriProducer(ctx, zebClient)
	extractedChan, extractionFailuresChan := docExtractor(ctx, zebClient, urisChan, maxConcurrentExtractions)
	transformedChan := docTransformer(extractedChan)
	indexedChan := docIndexer(ctx, esClient, transformedChan, maxConcurrentIndexings)

	summarize(indexedChan, extractionFailuresChan)
}

func uriProducer(ctx context.Context, z zebedeeClient) chan string {
	uriChan := make(chan string)
	go func() {
		defer close(uriChan)
		for _, uri := range getPublishedURIs(ctx, z) {
			for i := 0; i < 1; i++ {
				uriChan <- uri
			}
			//fmt.Printf("Sending %s to channel\n", uri)
		}
		fmt.Println("Finished listing uris")
	}()
	return uriChan
}

func getPublishedURIs(ctx context.Context, z zebedeeClient) []string {
	index, err := z.GetPublishedIndex(ctx)
	if err != nil {
		//TODO error handling
		log.Fatalf("Fatal error getting index from zebedee: %s", err)
	}
	fmt.Printf("Fetched %d uris from zebedee\n", len(index.URIs))
	return index.URIs
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
	var zebedeeData exporterModels.ZebedeeData
	err := json.Unmarshal(extractedDoc.Body, &zebedeeData)
	if err != nil {
		log.Fatal("error while attempting to unmarshal zebedee response into zebedeeData", err) //TODO proper error handling
	}

	exporterEventData := exporterModels.MapZebedeeDataToSearchDataImport(zebedeeData)
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

func docIndexer(ctx context.Context, es elasticSearchClient, transformedChan chan Document, maxIndexings int) chan bool {
	indexedChan := make(chan bool)
	go func() {
		defer close(indexedChan)

		indexName := createIndexName("ons")
		fmt.Printf("Index created: %s\n", indexName)
		status, err := es.CreateIndex(ctx, indexName, elasticsearch.GetSearchIndexSettings())
		if err != nil {
			log.Fatal(ctx, "error creating index", err)
		}
		if status != http.StatusOK {
			log.Fatal(ctx, "error creating index http status - ", status)
		}

		var wg sync.WaitGroup

		for w := 0; w < maxIndexings; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				indexDoc(ctx, es, transformedChan, indexedChan, indexName)
			}()
		}
		wg.Wait()
		fmt.Println("Finished indexing docs")
	}()
	return indexedChan
}

func createIndexName(s string) string {
	now := time.Now()
	return fmt.Sprintf("%s%d", s, now.UnixMicro())
}

func indexDoc(ctx context.Context, es elasticSearchClient, transformedChan chan Document, indexedChan chan bool, indexName string) {
	for transformedDoc := range transformedChan {

		id := url.PathEscape(transformedDoc.URI) //TODO this isn't right, the client should url-escape the id
		indexed := true
		status, err := es.AddDocument(ctx, indexName, "_create", id, transformedDoc.Body)
		if err != nil || status != http.StatusCreated {
			indexed = false
		}

		indexedChan <- indexed
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
