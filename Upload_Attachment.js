/* Node script to attach files to a record in any form.
*    
*   Install node.js from https://nodejs.org/download/
*	Install npm from https://www.npmjs.com/package/npm
*		npm install fs
*		npm install mime
*		npm install soap
*		npm install ya-csv
*
*	Download wsdls to WSDL_DIR. Set TABLE to form_name_for_attaching
*		https://<instance>.service-now.com/ecc_queue.do?WSDL
*		https://<instance>.service-now.com/form_name_for_attaching.do?WSDL
*
*	Copy the attachment files to DIR.
*
*	Setup the input csv file and the log file. 
*	First column of input csv file has a record identifier.
*	Second column has the filename.
*
*	Perform dry run, analyze the results and start the uploads.
*	
*/


var fs = require("fs"), // For reading files
	mime = require('mime'), //To determine filetype
	soap = require("soap"), //For soap calls
	csv = require("ya-csv"); //Handle csv files


var DIR = 'Files/'; //Folder containing the attachment files
var USERNAME = 'attachment_user'; //Credentials with privileges to create attachments
var PASSWORD = 'attachment_password';
var TABLE = 'ast_contract'; // Form to which files are to be attached
var RECORDID = 'u_uniqueid'//Field name on TABLE, values in the first column of csv
var CSVINFILE = 'ContractAttachments.csv'; //CSV file with filename and the record identifier
var CSVOUTFILE = 'ContractAttachmentsDryRun.csv'; //Output CSV file with attachment logs
var DRYRUN = true; // Run the script without actually attaching files. Use it for pre-run validation
var CHUNKS = 20; // Number of parallel uploads to run
var WSDL_DIR = 'wsdl-test/' //Folder with WSDLs


var writer = csv.createCsvFileWriter(CSVOUTFILE);  //CSV file to output logs
writer.writeRecord(['Type', 'Line No', 'Filename', 'Record Identifier', 'Message', 'ECC Sysid', 'Filesize']);


readCsv(function (err, results) {  //Start by reading the csv file
	if (err) console.error(err);
	if (results.length > CHUNKS) { //Parallelize for faster processing
		var i,j,csvchunk,chunk = Math.ceil(results.length/CHUNKS);// Split the array into chunks 
		for (i=0,j=results.length; i<j; i+=chunk) {
			csvchunk = results.slice(i,i+chunk);
			console.log('Log:Starting chunk',i);
			writer.writeRecord(['Log', i, '', '', 'Starting chunk']);
			attachFile(csvchunk); // For each chunk start attaching
			
		}
	} else { // Do not parallelize if the csv has too few records
	console.log('Log:Starting one chunk');
	writer.writeRecord(['Log', '', '', '', 'Starting one chunk']);
	attachFile(results); 
	}
});


function readCsv(callback)	{  //For reding the CSV file
	var csvlist = []; //Array to hold the csv file contents
	var reader = csv.createCsvFileReader(CSVINFILE, { //Open csv file to read
		'separator': ',',
		'quote': '"',
		'escape': '"',       
		'comment': '',
		columnsFromHeader: true, // Column names are taken from first row for simplicity
	});

	reader.on('data', function(data) {
		//console.log(data.uniqueid, data.file);
		csvlist.push([csvlist.length+2, data.uniqueid, data.file]); // Pushing each line into the array, include all columns of the csv file here
	});
	
	reader.on('end', function () {
		callback(null, csvlist); //Callback after reading the entire file
	});
	
	reader.on('error', function(err){
		reader.removeAllListeners(); //Close the file and exit on error.
		callback(err);
	});
}
	

function attachFile(csvlist) { 
	if(csvlist.length) { //Till all the lines are finished,
		var line = csvlist.pop(); //Take each line from the assigned chunk. 
		var lineno = line[0];
		var uniqueid = line[1]; // Extract record identifier
		var file = line[2]; //and the filename.
						
		fs.stat(DIR+file, function(err, stats) { //Check if the file existing 
			if (!err) {
				if (stats.isFile()) { //and is a valid file.  
					getSysid (uniqueid, file, csvlist, lineno); //If the file is OK, get the sysid(s) for the record identifier						
				} else {
					writer.writeRecord(['Error', lineno, uniqueid, file,'Not a file']);
					console.log('Error:Not a file:', file, ':', uniqueid); //If not a valid file,
					attachFile(csvlist); //call ourselves for recursive loop
				}
			} else {
				writer.writeRecord(['Error', lineno, uniqueid, file,'File not found']);
				console.log('Error: File not found:', file, ':', uniqueid); //If file not found,
				attachFile(csvlist); //Next line
			} 
		});
	
	} else {
		writer.writeRecord(['Log', '', '', '', 'Completed chunk']);
		console.log('Log:Completed chunk'); //All lines in the chunk processed
	}
}


function getSysid (uniqueid, file, csvlist, lineno) {
	soap.createClient(WSDL_DIR+TABLE+'.do.xml', function(err, client) { //SOAP query to get the sysid based on the record identifier
		if (err) console.error(err);
		client.setSecurity(new soap.BasicAuthSecurity(USERNAME, PASSWORD));
			var parameters = {
				RECORDID : uniqueid, //Additional record identifiers can be included
			};
		client.getKeys(parameters, function(err, result) { //Using getKeys request
			//console.log(parameters, result);
			if (err)  console.error(err);
			var sysidNo = Number(result.count); //Sysid(s) is/are returned in the comma separated string
			var sysidStr = result.sys_id[0]; //Extract into an araay
			var sysidArray = [];
			if (sysidNo) { 
				if (sysidNo > 1) {  
					sysidArray = sysidStr.split(',',sysidNo);			
				} else {
					sysidArray[0] = result.sys_id[0];
				}
				eccRequest(uniqueid, file, csvlist, lineno, sysidArray); //For each sysid, make SOAP request to ECC
			} else {
				writer.writeRecord(['Error', lineno, file, uniqueid, 'Record identifier not found']);
				console.log ('Error:Record identifier not found:', file, ':', uniqueid);
				attachFile(csvlist); //Next line
			} 
		});
	});
}


function eccRequest(uniqueid, file, csvlist, lineno, sysidArray) { // SOAP request to attach files
	if (sysidArray.length) { //Till all the sysids are processed
		var sysid = sysidArray.pop();
		var filetype = mime.lookup(file); //Determine the file type from the file name
		fs.readFile(DIR+file, function(err, filedata) { //Read the file and 
			if (err) console.error(err);
			var payload = new Buffer(filedata, 'binary').toString('base64');//encode into base64.
			soap.createClient(WSDL_DIR+'ecc_queue.do.xml', function(err, client) { //Create a SOAP client
				if (err) console.error(err);
				client.setSecurity(new soap.BasicAuthSecurity(USERNAME, PASSWORD));
				var parameters = { //with the requqest parameters
					'agent': 	'AttachmentCreator', 
					'topic': 	'AttachmentCreator',
					'name': 	file+':'+filetype, //filename and the mime type
					'source': 	TABLE+':'+sysid, //form and the sysid of the record
					'payload': 	payload //and the encoded data.
					};
				if(DRYRUN) { //Do not upload file if it is a dryrun.
					writer.writeRecord(['Info', lineno, file, uniqueid, 'File to be atttached', sysid, payload.length]);
					console.log('Info:File to be attached:', file, ':', ':', sysid, ':', payload.length);
					eccRequest(uniqueid, file, csvlist, lineno, sysidArray); //Loop through all sysids
				} else {
					client.insert(parameters, function(err, result) { //Insert into ECC Queue
						if (err) console.error(err);
						writer.writeRecord(['Info', lineno, file, uniqueid, 'Attachment request sent', sysid, payload.length, result.sys_id]);
						console.log('Info:File attached:', file, ':', ':', sysid, ':', payload.length, ':', result.sys_id);
						eccRequest(uniqueid, file, csvlist, lineno, sysidArray);						
					});
				}
			});
			
		});
	} else {
	attachFile(csvlist); //Next line
	}
}
