package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
)

var db map[string]string

func handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" || r.Header.Get("Authorization") == "" {
		log.Println("Unauthorized request")
		return
	}
	switch t := r.Header.Get("X-Amz-Target"); t {
	case "AmazonSSM.PutParameter":
		defer r.Body.Close()
		var b ssm.PutParameterInput
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&b)
		if err != nil {
			panic(err)
		}
		log.Printf("Body=%#v\n", b)
		log.Printf("DB: Setting key=%s value=%s", *b.Name, *b.Value)
		db[*b.Name] = *b.Value
		return
	case "AmazonSSM.GetParametersByPath":
		defer r.Body.Close()
		var b ssm.GetParametersByPathInput
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&b)
		if err != nil {
			panic(err)
		}
		log.Printf("Body=%#v\n", b)
		parameters := make([]types.Parameter, 0, 0)
		path := b.Path
		for k, v := range db {
			if strings.HasPrefix(k, *path+"/") == false {
				continue
			}
			parameters = append(parameters, types.Parameter{
				Name:  aws.String(k),
				Type:  types.ParameterTypeString,
				Value: aws.String(v),
			})
		}
		var GetParametersByPathOutput ssm.GetParametersByPathOutput
		GetParametersByPathOutput.Parameters = parameters
		resp, err := json.Marshal(GetParametersByPathOutput)
		if err != nil {
			panic(err)
		}
		fmt.Fprint(w, string(resp))
		return
	case "AmazonSSM.GetParameter":
		defer r.Body.Close()
		var b ssm.GetParameterInput
		decoder := json.NewDecoder(r.Body)
		err := decoder.Decode(&b)
		if err != nil {
			panic(err)
		}
		log.Printf("Body=%#v\n", b)
		var GetParameterOutput ssm.GetParameterOutput
		parameter := &types.Parameter{
			Name:  aws.String(*b.Name),
			Type:  types.ParameterTypeString,
			Value: aws.String(db[*b.Name]),
		}
		GetParameterOutput.Parameter = parameter
		resp, err := json.Marshal(GetParameterOutput)
		if err != nil {
			panic(err)
		}
		fmt.Fprint(w, string(resp))
		return
	default:
		log.Println("Unknown target")
		return
	}
}

func main() {
	db = make(map[string]string)
	http.HandleFunc("/", handler)
	log.Println("Starting AWS SSM HTTP mocking server")
	http.ListenAndServe(":8001", nil)
}
