const should = require('chai').should(),
    assert = require('chai').assert,
    fs = require('fs'),
    CkanSyncClient = require('../ckan-sync'),
    ckanTestEndpoint = "http://dendro-dev.fe.up.pt:5000/",
    ckanTestApiKey = "cd510ee4-d0fc-4996-b59e-f411acf3b308";
let client;


describe("Create a Ckan Client and get info from the organization 'organization-test1' with success", function () {
    client = new CkanSyncClient(ckanTestEndpoint, ckanTestApiKey);

    it("Should show that the user has the organization 'organization-test1'", function (done) {
        done(1);
    });
});

/*
describe('#B2ShareClient with success', function () {
    var client;
    var recordIDToUpdate;
    var bucketUrlToListFiles;
    var fileBucketID;
    before(function () {
        var host = 'trng-b2share.eudat.eu';
        var accessTokens = 'MmGKBzjpdlT382lag38zxhsKttZDw9e7u6zZmzucVFUu1aYM5i55WpeUSgFE';
        client = new B2ShareClient(host, accessTokens);
    });
    it('listCommunities with success', function (done) {
        var expectedStatusCode = 200;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            client.listCommunities(function (err, body) {
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('getCommunitySchema with success', function (done) {
        var expectedStatusCode = 200;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            var communityID = '0afede87-2bf2-4d89-867e-d2ee57251c62';
            client.getCommunitySchema(communityID, function (err, body) {
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('listAllRecords with success', function (done) {
        var expectedStatusCode = 200;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            client.listAllRecords(function (err, body) {
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('listRecordsPerCommunity with success', function (done) {
        var expectedStatusCode = 200;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            var communityID = '0afede87-2bf2-4d89-867e-d2ee57251c62';
            client.listRecordsPerCommunity(communityID,function (err, body) {
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('searchRecords with success', function (done) {
        var expectedStatusCode = 200;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            var queryString = '0afede87-2bf2-4d89-867e-d2ee57251c62';
            client.searchRecords(queryString,function (err, body) {
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('searchDrafts with success', function (done) {
        var expectedStatusCode = 200;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            client.searchDrafts(function (err, body) {
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('getSpecificRecord with success', function (done) {
        var expectedStatusCode = 200;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            var recordID = 'a1c2ef96a1e446fa9bd7a2a46d2242d4';
            client.getSpecificRecord(recordID, function (err, body) {
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('createADraftRecord with success', function (done) {
        var expectedStatusCode = 201;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            var data = {"titles":[{"title":"NEWTESTFIXE"}], "community":"e9b9792e-79fb-4b07-b6b4-b9c2bd06d095", "open_access":true, "community_specific": {}};
            client.createADraftRecord(data, function (err, body) {
                recordIDToUpdate = body.data.id;
                bucketUrlToListFiles = body.data.links.files;
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('uploadFileIntoDraftRecord with success', function (done) {
        this.timeout(5000);
        var expectedStatusCode = 200;
        fileBucketID = bucketUrlToListFiles.split('/').pop();
        fs.readFile(__dirname + '/testFile.txt', function (err, data) {
            if(!err)
            {
                assert.doesNotThrow(function () {
                    var info = {"fileBucketID":fileBucketID, "fileNameWithExt": "testFile.txt"};
                    client.uploadFileIntoDraftRecord(info, data, function (err, body) {
                        assert.equal(body.statusCode, expectedStatusCode);
                        done();
                    }, done);
                });
            }
            else
            {
                console.log('there was an error reading the file');
                console.log(err);
                done(err);
            }
        });

    });

    it('listUploadedFilesInRecord with success', function (done) {
        var expectedStatusCode = 200;
        this.timeout(5000);
        var buffer = [];
        assert.doesNotThrow(function () {
            client.listUploadedFilesInRecord(fileBucketID, function (err, body) {
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });


    it('updateDraftRecordMetadata with success', function (done) {
        var expectedStatusCode = 200;
        this.timeout(5000);
        var jsonPatchFormatData = [
            { "op": "replace", "path": "/titles/0/title", "value": "TESTEBONITO2" }
        ];

        assert.doesNotThrow(function () {
            client.updateDraftRecordMetadata(recordIDToUpdate, jsonPatchFormatData, function (err, body) {
                recordIDToUpdate = body.data.id;
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('submitDraftRecordForPublication with success', function (done) {
        var expectedStatusCode = 200;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            client.submitDraftRecordForPublication(recordIDToUpdate, function (err, body) {
                recordIDToUpdate = body.data.id;
                bucketUrlToListFiles = body.data.links.files;
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });
});


describe('#B2ShareClient with errors', function () {
    var client;
    var recordIDToUpdate;
    var bucketUrlToListFiles;
    var fileBucketID;
    before(function () {
        var host = 'Errortrng-b2share.eudat.eu';
        var accessTokens = 'MmGKBzjpdlT382lag38zxhsKttZDw9e7u6zZmzucVFUu1aYM5i55WpeUSgFE';
        client = new B2ShareClient(host, accessTokens);
    });
    it('listCommunities Error', function (done) {
        var expectedStatusCode = 500;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            client.listCommunities(function (err, body) {
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('getCommunitySchema Error', function (done) {
        var expectedStatusCode = 500;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            var communityID = '0afede87-2bf2-4d89-867e-d2ee57251c62';
            client.getCommunitySchema(communityID, function (err, body) {
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('listAllRecords Error', function (done) {
        var expectedStatusCode = 500;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            client.listAllRecords(function (err, body) {
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('listRecordsPerCommunity Error', function (done) {
        var expectedStatusCode = 500;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            var communityID = '0afede87-2bf2-4d89-867e-d2ee57251c62';
            client.listRecordsPerCommunity(communityID,function (err, body) {
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('searchRecords Error', function (done) {
        var expectedStatusCode = 500;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            var queryString = '0afede87-2bf2-4d89-867e-d2ee57251c62';
            client.searchRecords(queryString,function (err, body) {
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('searchDrafts Error', function (done) {
        var expectedStatusCode = 500;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            client.searchDrafts(function (err, body) {
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('getSpecificRecord Error', function (done) {
        var expectedStatusCode = 500;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            var recordID = 'a1c2ef96a1e446fa9bd7a2a46d2242d4';
            client.getSpecificRecord(recordID, function (err, body) {
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('createADraftRecord Error', function (done) {
        var expectedStatusCode = 500;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            var data = {"titles":[{"title":"NEWTESTFIXE"}], "community":"e9b9792e-79fb-4b07-b6b4-b9c2bd06d095", "open_access":true, "community_specific": {}};
            client.createADraftRecord(data, function (err, body) {
                //recordIDToUpdate = body.data.id;
                recordIDToUpdate = "12345";
                //bucketUrlToListFiles = body.data.links.files;
                bucketUrlToListFiles = 'thisIsForTestPurposes/123456';
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('uploadFileIntoDraftRecord Error', function (done) {
        this.timeout(5000);
        var expectedStatusCode = 500;
        fileBucketID = bucketUrlToListFiles.split('/').pop();
        fs.readFile(__dirname + '/testFile.txt', function (err, data) {
            if(!err)
            {
                assert.doesNotThrow(function () {
                    var info = {"fileBucketID":fileBucketID, "fileNameWithExt": "testFile.txt"};
                    client.uploadFileIntoDraftRecord(info, data, function (err, body) {
                        assert.equal(body.statusCode, expectedStatusCode);
                        done();
                    }, done);
                });
            }
            else
            {
                console.log('there was an error reading the file');
                console.log(err);
                done(err);
            }
        });

    });

    it('listUploadedFilesInRecord Error', function (done) {
        var expectedStatusCode = 500;
        this.timeout(5000);
        var buffer = [];
        assert.doesNotThrow(function () {
            client.listUploadedFilesInRecord(fileBucketID, function (err, body) {
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });


    it('updateDraftRecordMetadata Error', function (done) {
        var expectedStatusCode = 500;
        this.timeout(5000);
        var jsonPatchFormatData = [
            { "op": "replace", "path": "/titles/0/title", "value": "TESTEBONITO2" }
        ];

        assert.doesNotThrow(function () {
            client.updateDraftRecordMetadata(recordIDToUpdate, jsonPatchFormatData, function (err, body) {
                //recordIDToUpdate = body.data.id;
                recordIDToUpdate = '12345';
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });

    it('submitDraftRecordForPublication Error', function (done) {
        var expectedStatusCode = 500;
        this.timeout(5000);

        assert.doesNotThrow(function () {
            client.submitDraftRecordForPublication(recordIDToUpdate, function (err, body) {
                //recordIDToUpdate = body.data.id;
                recordIDToUpdate = '12345';
                //bucketUrlToListFiles = body.data.links.files;
                bucketUrlToListFiles = 'thisIsForTestPurposes/12345';
                assert.equal(body.statusCode, expectedStatusCode);
                done();
            }, done);
        });

    });
});*/
