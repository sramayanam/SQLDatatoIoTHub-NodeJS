///////The program reads the stream from SQL and send data to an IoT Device//////
///////This code is Written for Demonstration and Educational purpose///////////
///////This code should not be used for production purposes         ///////////

//MQTT is a simple protocol, it accepts the message 
//by default and there is no notion of rejecting or abandoning the message
const Protocol = require('azure-iot-device-mqtt').Mqtt;
//Client Sends messages to IoT hub in this case this is our desktop application
const Client = require('azure-iot-device').Client;

//Fetch the DB Credentials from config file that is locked down at enterprise level
var config = require("./config");
const user = config.names.user;
const password = config.names.password;
const server = config.names.server;
const database = config.names.database;

//Fetch the IoT hub device Credentials for sending the streaming data
const connectionString = config.connection.connectionString;

//Get a handle on IoT client based on connectionstring and protocol
var client = Client.fromConnectionString(connectionString, Protocol);
var Message = require('azure-iot-device').Message; //Message to be sent to IoT device

//Microsoft SQL Server client for Node.js - Tedious based driver - Platform Agnostic
const sql = require('mssql')

//sqlconfiguration to connect to an Azure SQL Database
const sqlConfig = {
    user: user,
    password: password,
    server: server,
    database: database,
    encrypt: true //mandatory for Azure SQL
}

//The main callback after IoT connection is established
var connectCallback = function(err) {
    if (err) {
        console.error('Could not connect to IoT Hub: ' + err.message);
    } else {
        console.log('Client connected to IoT hub');
        //Make a SQL Connection to read the data from a large table
        sql.connect(sqlConfig, err => {
            const request = new sql.Request()
            //Streaming has to be set to true for large datasets by using this flag
            request.stream = true 
            //if Executing a stored procedure replace request.query with request.execute(sp name)
            request.query('SELECT TOP 50 * FROM [dbo].[weather]') 
            let rowsToProcess = [];
            request.on('row', row => {
                rowsToProcess.push(row);
                //Read Batches of 10 rows and process 10 rows at a time
                if (rowsToProcess.length >= 10) {
                    request.pause();
                    console.log("Processing the Rows aka sending to IoT Hub");
                    processRows();
                }
            });
            request.on('error', err => {
                console.log("printing the error in request.on.error", err);
            })
            request.on('done', () => {
                processRows();
                console.log("### Finished Processing everything ###");
            });

            //This function processes mini batches
            function processRows() {
                //Read the Array hydrated one row at a time
                rowsToProcess.forEach(row => {
                    var message = new Message(JSON.stringify(row));
                    console.log("Printing Message.........", message);
                    var cDttm = new Date();
                    //Send the Message aka event to the IoTHub
                    client.sendEvent(message, function (error) {
                        if (error) {
                            console.log(error.toString());
                        } else {
                            console.log("Data sent to IoT Hub  on %s...", cDttm);
                        }
                    });
                    
                });
                //Reset the array to be hydrated for the new mini batch
                rowsToProcess = [];
                //Remove all listeners to prevent memory leaks
                client.removeAllListeners();
                //Resume Database Processing
                request.resume();
            }

        })

        sql.on('error', err => {
            console.log("printing the error in sql.on.error", err);
        })

        client.on('error', function (err) {
            console.error(err.message);
        });

        client.on('disconnect', function () {
            client.removeAllListeners();
            client.open(connectCallback);
        });
    }
};

//Main Call for the entire ensemble
client.open(connectCallback);