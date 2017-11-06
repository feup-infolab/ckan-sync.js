const ckan = require("ckan");
const async = require("async");
const mime = require("mime-types");
const fs = require("fs");
const slugify = require("slugify");
const path = require("path");

/**
 * Initiates the ckanSyncClient
 * @param endpoint the host required to execute the requests(ex: https://demo.ckan.org)
 * @param apiKey the user's access token
 * @constructor
 */
function CkanSyncClient(endpoint, apiKey) {
    this.endpoint = endpoint;
    this.apiKey = apiKey;
    this.ckanClient = new ckan.Client(endpoint, apiKey);
}

/**
 * Get a list of resources that where changed after a certain date in a dataset
 * @param lastSyncDate the date of the last time the changes were synced
 * @param packageId Id of the ckan dataset (package) to be checked
 * @param callback Response callback
 */

CkanSyncClient.prototype.getChangesInDatasetAfterDate = function(
    lastSyncDate,
    packageId,
    callback)
{
    let self = this;
    let dateObject;
    try {
        dateObject = new Date(lastSyncDate);
        lastSyncDate = dateObject.toISOString();
    }
    catch (e)
    {
        let message = "Invalid format for lastSyncDate. Should be in ISO format (ISO 8601).";
        console.error(message);
        return callback(true, message);
    }

    self.ckanClient.action("package_show",
        {
            id: packageId
        },
        function (err, result) {
            if (result.success) {
                let folderResourcesInCkan = result.result.resources;
                let changedResources = [];
                async.map(folderResourcesInCkan, function(resource, callback){
                    if(resource.last_modified > lastSyncDate)
                    {
                        changedResources.push(resource);
                    }
                    callback(null, resource);
                }, function(err, results){
                    result.result.changedResources = changedResources;
                    callback(err, result);
                });
            }
            else {
                callback(err, result);
            }
        });
};


/**
 * Uploads a series of files into the CKAN repository
 * @param resources Array of resources to upload
 * @param packageId Id of the ckan dataset (package) into which to upload / update the files
 * @param callback Response callback
 */

CkanSyncClient.prototype.upload_files_into_package = function(
    resources,
    packageId,
    callback)
{
    let self = this;

    const uploadFile = function(resource, cb)
    {
        self.upload_file_into_package(
            resource.absolute_file_path,
            packageId,
            resource.description,
            resource.filename,
            resource.extension,
            resource.format,
            function(err, result){
                if(!err)
                {
                    cb(err, result);
                }
                else
                {
                    cb(err, result);
                }
            },
            resource.resourceUrl,
            resource.mimetype,
            resource.overwrite_if_exists,
            resource.id
        );
    };

    async.mapSeries(resources, uploadFile, function(err, results){
        callback(err, results);
    });
};

/**
 * Uploads a file into a CKAN Dataset
 * @param absolutePathToFileToBeUploaded
 * @param packageId ID of the dataset into which the file needs to be uploaded
 * @param description Description of the file
 * @param fileName Full name of the file (i.e. photo.png)
 * @param extension File extension of the uploaded file, without the dot (i.e png, not .png)
 * @param format Format of the uploaded file, Typically extension in UPPERCASE LETTERS
 * @param callback
 * @param {string} [resourceUrl] Final URL of the uploaded resource (typically http://ckan-server.com/dataset/ >>>>>packageID<<<<<< /resource/ >>>>>FileName<<<<<
 * @param {string} [mimetype] of the uploaded file
 * @param {boolean} [overwriteIfExists] Will overwrite a file if it exists in the @packageId
 * @param {string} [resourceId] resource ID of the resource to be uploaded
 */

CkanSyncClient.prototype.upload_file_into_package = function(
    absolutePathToFileToBeUploaded,
    packageId,
    description,
    fileName,
    extension,
    format,
    callback,
    resourceUrl,
    mimetype,
    overwriteIfExists,
    resourceId
)
{
    let self = this;
    if(resourceUrl == null)
    {
        resourceUrl = self.host + "/dataset/" + packageId + "/resource/" + fileName;
    }

    if(mimetype == null)
    {
        mimetype = mime.lookup(extension);
    }

    if(fileName == null)
    {
        fileName = path.basename(absolutePathToFileToBeUploaded);
    }

    if(overwriteIfExists === null)
    {
        overwriteIfExists = false;
    }

    if(resourceId == null)
    {
        resourceId = slugify(fileName);
    }

    if(format == null)
    {
        format = extension.toUpperCase();
    }

    let file = {
        url : resourceUrl,
        package_id : packageId,
        description: description || "< no description available >",
        name: fileName,
        mimetype: mimetype,
        extension : extension,
        id : resourceId,
        absolute_file_path : absolutePathToFileToBeUploaded,
        format : format
    };

    const checkIfResourceExists = function(file, callback)
    {
        let queryString = "res_url: \""+file.url+"\"";

        self.ckanClient.action("package_search",
            {
                fq : queryString
            },
            function(err, response)
            {
                if(!err && response.result != null)
                {
                    if(
                        response != null &&
                        response.result != null &&
                        response.result.results != null &&
                        response.result.results instanceof Array &&
                        response.result.results.length == 1
                    )
                    {
                        callback(null, response.result.results[0].id);
                    }
                    else
                    {
                        callback(null, null);
                    }
                }
                else
                {
                    callback(1, response.result);
                }
            });
    };

    const createResourceInPackage = function(callback)
    {
        self.ckanClient.action("resource_create",
            file,
            function (err, response)
            {
                if (response.success)
                {
                    fs.stat(file.absolute_file_path, function(err, stats){
                        if(err == null)
                        {
                            var formData =  {
                                id: response.result.id,
                                upload: fs.createReadStream(file.absolute_file_path),
                                format : file.format,
                                name : file.name,
                                description : file.description,
                                url : response.result.url,
                                package_id : file.package_id
                            };

                            request.post(
                                {
                                    url:self.endpoint +"/action/resource_create",
                                    formData: formData,
                                    headers : {
                                        Authorization: self.apiKey
                                    }
                                },function(err, response, body) {
                                    if(!err)
                                    {
                                        if(response != null && response.statusCode == 200)
                                        {
                                            let jsonResponse;
                                            try{
                                                jsonResponse = JSON.parse(response.toJSON().body);
                                            }
                                            catch(e)
                                            {
                                                let msg = "Upload complete but there was an error parsing the response from the CKAN repository.";
                                                return callback(null, msg);
                                            }
                                            callback(null, jsonResponse);
                                        }
                                        else
                                        {
                                            callback(1, response.body)
                                        }
                                    }
                                    else
                                    {
                                        if(err != null && response != null && response.success)
                                        {
                                            callback(null, body);
                                        }
                                        else
                                        {
                                            callback(1, "Unknown error occurred uploading file to CKAN");
                                        }
                                    }

                                });
                        }
                        else if(err.code === "ENOENT")
                        {
                            callback(1, "File " + absolutePathToFileToBeUploaded + " does not exist.");
                        }
                        else
                        {
                            console.log("Some other error: ", err.code);
                        }
                    });
                }
                else
                {
                    callback(err, response);
                }
            }
        );
    };

    const updateResourceInPackage = function(file, callback)
    {
        self.ckanClient.action("resource_update",
            file,
            function (err, response)
            {
                if (response.success)
                {
                    fs.stat(file.absolute_file_path, function(err, stats){
                        if(err == null)
                        {
                            var formData =  {
                                id: response.result.id,
                                upload: fs.createReadStream(file.absolute_file_path),
                                format : file.formate,
                                name : file.name,
                                description : file.description,
                                url : response.result.url,
                                package_id : response.result.package_id
                            };

                            request.post(
                                {
                                    url:self.endpoint +"/action/resource_update",
                                    formData: formData,
                                    headers : {
                                        Authorization: self.apiKey
                                    }
                                },function(err, response, body) {
                                    if(!err)
                                    {
                                        if(response != null && response.statusCode == 200)
                                        {
                                            let jsonResponse;
                                            try{
                                                // callback(null, JSON.parse(response.toJSON().body));
                                                jsonResponse = JSON.parse(response.toJSON().body);
                                            }
                                            catch(e)
                                            {
                                                let msg = "Upload complete but there was an error parsing the response from the CKAN repository.";
                                                return callback(null, msg);
                                            }
                                            callback(null, jsonResponse);
                                        }
                                        else
                                        {
                                            callback(1, response.result)
                                        }
                                    }
                                    else
                                    {
                                        if(err != null && response != null && response.success)
                                        {
                                            callback(null, body);
                                        }
                                        else
                                        {
                                            callback(1, "Unknown error occurred uploading file to CKAN");
                                        }
                                    }

                                });
                        }
                        else if(err.code === "ENOENT") {
                            callback(1, "File " + absolutePathToFileToBeUploaded + " does not exist.");
                        } else
                        {
                            callback(1, err);
                        }
                    });
                }
                else
                {
                    callback(err, response);
                }
            }
        );
    };

    async.waterfall([
        function(cb)
        {
            checkIfResourceExists(file, cb);
        },
        function(existingResourceId, cb)
        {
            if(existingResourceId != null)
            {
                file.id = existingResourceId;
                if(overwriteIfExists)
                {
                    updateResourceInPackage(file, cb);
                }
                else
                {
                    cb(1, "Resource already exists in the package and the overwrite flag was not specified.");
                }
            }
            else
            {
                createResourceInPackage(cb);
            }
        }
    ], function(err, results){
        callback(err, results);
    });
};

