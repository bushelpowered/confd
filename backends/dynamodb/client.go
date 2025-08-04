package dynamodb

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/kelseyhightower/confd/log"
)

// Client is a wrapper around the DynamoDB client
// and also holds the table to lookup key value pairs from
type Client struct {
	client *dynamodb.Client
	table  string
}

// NewDynamoDBClient returns an *dynamodb.Client with a connection to the region
// configured via the AWS_REGION environment variable.
// It returns an error if the connection cannot be made or the table does not exist.
func NewDynamoDBClient(table string) (*Client, error) {
	var cfg aws.Config
	var err error

	if os.Getenv("DYNAMODB_LOCAL") != "" {
		log.Debug("DYNAMODB_LOCAL is set")
		cfg, err = config.LoadDefaultConfig(context.TODO(),
			config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
				func(service, region string, options ...interface{}) (aws.Endpoint, error) {
					return aws.Endpoint{URL: "http://localhost:8000"}, nil
				})))
	} else {
		cfg, err = config.LoadDefaultConfig(context.TODO())
	}

	if err != nil {
		return nil, err
	}

	d := dynamodb.NewFromConfig(cfg)

	// Check if the table exists
	_, err = d.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{TableName: &table})
	if err != nil {
		return nil, err
	}
	return &Client{d, table}, nil
}

// GetValues retrieves the values for the given keys from DynamoDB
func (c *Client) GetValues(keys []string) (map[string]string, error) {
	vars := make(map[string]string)
	for _, key := range keys {
		// Check if we can find the single item
		m := make(map[string]types.AttributeValue)
		m["key"] = &types.AttributeValueMemberS{Value: key}
		g, err := c.client.GetItem(context.TODO(), &dynamodb.GetItemInput{Key: m, TableName: &c.table})
		if err != nil {
			return vars, err
		}

		if g.Item != nil {
			if val, ok := g.Item["value"]; ok {
				if s, ok := val.(*types.AttributeValueMemberS); ok {
					vars[key] = s.Value
				} else {
					log.Warning("Skipping key '%s'. 'value' is not of type 'string'.", key)
				}
				continue
			}
		}

		// Check for nested keys
		q, err := c.client.Scan(context.TODO(),
			&dynamodb.ScanInput{
				FilterExpression: aws.String("begins_with(#k, :key)"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":key": &types.AttributeValueMemberS{Value: key},
				},
				ProjectionExpression: aws.String("#k, #v"),
				ExpressionAttributeNames: map[string]string{
					"#k": "key",
					"#v": "value",
				},
				TableName: aws.String(c.table),
			})

		if err != nil {
			return vars, err
		}

		for _, i := range q.Items {
			item := i
			if val, ok := item["value"]; ok {
				if s, ok := val.(*types.AttributeValueMemberS); ok {
					if keyVal, ok := item["key"]; ok {
						if keyS, ok := keyVal.(*types.AttributeValueMemberS); ok {
							vars[keyS.Value] = s.Value
						}
					}
				} else {
					if keyVal, ok := item["key"]; ok {
						if keyS, ok := keyVal.(*types.AttributeValueMemberS); ok {
							log.Warning("Skipping key '%s'. 'value' is not of type 'string'.", keyS.Value)
						}
					}
				}
				continue
			}
		}
	}
	return vars, nil
}

// WatchPrefix is not implemented
func (c *Client) WatchPrefix(prefix string, keys []string, waitIndex uint64, stopChan chan bool) (uint64, error) {
	<-stopChan
	return 0, nil
}
