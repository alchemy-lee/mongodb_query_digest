package main

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/howeyc/gopass"
	"github.com/pborman/getopt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/alchemy-lee/mongodb_query_digest/proto"
)

const (
	DEFAULT_AUTHDB          = "admin"
	DEFAULT_HOST            = "localhost:27017"
	DEFAULT_LOGLEVEL        = "warn"
	DEFAULT_ORDERBY         = "-count"         // comma separated list
	DEFAULT_SKIPCOLLECTIONS = "system.profile" // comma separated list
)

var excludedField = make(map[string]bool)

var typeInfo bool

type cliOptions struct {
	// TODO: check AuthDB option usage
	AuthDB          string
	Database        string
	Help            bool
	Host            string
	LogLevel        string
	Password        string
	SkipCollections []string
	SSLCAFile       string
	SSLPEMKeyFile   string
	User            string
	TypeInfo        bool
}

func main() {
	excludedField["multi"] = true
	excludedField["upsert"] = true
	excludedField["ordered"] = true
	excludedField["$readPreference"] = true
	excludedField["$writePreference"] = true
	// excludedField["batchSize"] = true
	excludedField["remove"] = true

	opts, err := getOptions()
	if err != nil {
		log.Errorf("error processing command line arguments: %s", err)
		os.Exit(1)
	}
	if opts == nil && err == nil {
		return
	}

	log.Debugf("Command line options:\n%+v\n", opts)

	clientOptions, err := getClientOptions(opts)
	if err != nil {
		log.Errorf("Cannot get a MongoDB client: %s", err)
		os.Exit(2)
	}

	log.Debugf("Dial Info: %+v\n", clientOptions)

	ctx := context.Background()

	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		log.Fatalf("Cannot create a new MongoDB client: %s", err)
	}

	if err := client.Connect(ctx); err != nil {
		log.Fatalf("Cannot connect to MongoDB: %s", err)
	}

	cursor, err := client.Database(opts.Database).Collection("system.profile").Find(ctx, primitive.M{})
	if err != nil {
		panic(err)
	}

	getDocs(ctx, cursor)

}

func conv_bsonA(arr bson.A) bson.A {
	if len(arr) == 0 {
		return nil
	}

	doc := bson.A{}
	for _, elem := range arr {
		switch v := elem.(type) {
		case bson.D:
			doc = append(doc, conv_bsonD(v))
		case bson.A:
			doc = append(doc, conv_bsonA(v))
		default:
			doc = append(doc, "***")
		}
	}
	return doc
}

func conv_bsonD(query bson.D) bson.D {
	if len(query) == 0 {
		return nil
	}

	doc := bson.D{}
	for _, elem := range query {
		key := elem.Key
		if key[0] != '$' {
			key = "***"
		}
		doc = append(doc, bson.E{Key: key, Value: conv_bsonE(elem)})
	}
	return doc
}

func conv_bsonE(elem bson.E) interface{} {
	if elem.Value == nil {
		return nil
	}

	switch v := elem.Value.(type) {
	case bson.D:
		return conv_bsonD(v)
	case bson.A:
		return conv_bsonA(v)
	default:
		// ues type instead of ***
		if typeInfo {
			return reflect.TypeOf(elem.Value).Name()
		} else {
			return "***"
		}
	}
}

func conv_root(query bson.D) bson.D {
	if len(query) == 0 {
		return nil
	}

	doc := bson.D{}
	for _, elem := range query {
		if _, ok := excludedField[elem.Key]; ok {
			doc = append(doc, bson.E{Key: elem.Key, Value: elem.Value})
		} else {
			doc = append(doc, bson.E{Key: elem.Key, Value: conv_bsonE(elem)})
		}
	}
	return doc
}

func getDocs(ctx context.Context, cursor *mongo.Cursor) {
	var query proto.SystemProfile

	for cursor.Next(ctx) {
		if err := cursor.Decode(&query); err != nil {
			log.Fatalf("Cannot get data from cursor: %s", err)
			return
		}

		if strings.HasSuffix(query.Ns, DEFAULT_SKIPCOLLECTIONS) {
			continue
		}

		// TODO: maybe use query.Query
		doc := conv_root(query.Command)
		if doc == nil {
			continue
		}

		originalBson, _ := bson.MarshalExtJSON(query.Command, true, true)
		fmt.Printf("脱敏前 %s\n", string(originalBson))

		queryBson, err := bson.MarshalExtJSON(doc, true, true)
		if err != nil {
			log.Fatalf("Cannot transform BSON to JSON: %s", err)
		}
		fmt.Printf("脱敏后 %s: %s\n", query.Op, string(queryBson))
	}
}

func getClientOptions(opts *cliOptions) (*options.ClientOptions, error) {
	clientOptions := options.Client().ApplyURI(opts.Host)
	credential := options.Credential{}
	if opts.User != "" {
		credential.Username = opts.User
		clientOptions.SetAuth(credential)
	}
	if opts.Password != "" {
		credential.Password = opts.Password
		credential.PasswordSet = true
		clientOptions.SetAuth(credential)
	}
	return clientOptions, nil
}

func getOptions() (*cliOptions, error) {
	opts := &cliOptions{
		Host:            DEFAULT_HOST,
		LogLevel:        DEFAULT_LOGLEVEL,
		SkipCollections: strings.Split(DEFAULT_SKIPCOLLECTIONS, ","),
		AuthDB:          DEFAULT_AUTHDB,
	}

	gop := getopt.New()
	gop.BoolVarLong(&opts.Help, "help", '?', "Show help")

	gop.StringVarLong(&opts.Host, "host", 'h', "MongoDB host:port")
	gop.ListVarLong(&opts.SkipCollections, "skip-collections", 's', "A comma separated list of collections (namespaces) to skip."+
		"  Default: "+DEFAULT_SKIPCOLLECTIONS)

	gop.StringVarLong(&opts.AuthDB, "authenticationDatabase", 'a', "admin", "Database to use for optional MongoDB authentication. Default: admin")
	gop.StringVarLong(&opts.Database, "database", 'd', "", "MongoDB database to profile")
	gop.StringVarLong(&opts.LogLevel, "log-level", 'l', "Log level: error", "panic, fatal, error, warn, info, debug. Default: error")
	gop.StringVarLong(&opts.Password, "password", 'p', "", "Password to use for optional MongoDB authentication").SetOptional()
	gop.StringVarLong(&opts.User, "username", 'u', "Username to use for optional MongoDB authentication")
	gop.StringVarLong(&opts.SSLCAFile, "sslCAFile", 0, "SSL CA cert file used for authentication")
	gop.StringVarLong(&opts.SSLPEMKeyFile, "sslPEMKeyFile", 0, "SSL client PEM file used for authentication")
	gop.BoolVarLong(&opts.TypeInfo, "typeinfo", 't', "Include type information in output")

	gop.SetParameters("host[:port]")

	gop.Parse(os.Args)

	if opts.Help {
		gop.PrintUsage(os.Stdout)
		return nil, nil
	}

	if gop.IsSet("password") && opts.Password == "" {
		print("Password: ")
		pass, err := gopass.GetPasswd()
		if err != nil {
			return nil, err
		}
		opts.Password = string(pass)
	}

	if !strings.HasPrefix(opts.Host, "mongodb://") {
		opts.Host = "mongodb://" + opts.Host
	}

	if opts.Database == "" {
		log.Errorln("must indicate a database to profile with the --database parameter")
		getopt.PrintUsage(os.Stderr)
		os.Exit(2)
	}

	typeInfo = opts.TypeInfo

	return opts, nil
}
