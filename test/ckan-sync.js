const chai = require("chai");
const should = chai.should();
const fs = require('fs');
const CkanSyncClient = require('../ckan-sync');
const ckanTestEndpoint = "http://dendro-dev.fe.up.pt:5000/";
const ckanTestApiKey = "cd510ee4-d0fc-4996-b59e-f411acf3b308";
const organizationMock = require("./mocks/organization");
const packageMock = require("./mocks/package");
//const ckanClientForDatasetPurging = require("ckanInfoLab");
let SyncClient;

describe("The tests for the npm package 'ckan-sync.js'", function () {

    //TODO needs to purge the dataset before running the tests, otherwise it will give an error saying that a package with that id already exists(because the tests already created the package with that id in a previous test instance)
    /*before(function (done) {
        //destroy graphs
        let clientForPurging = new ckanClientForDatasetPurging(ckanTestEndpoint, ckanTestApiKey);
        clientForPurging.action("dataset_purge",
            {
                id: packageMock.name
            },
            function (err, result) {
                should.equal(err, null);
                done();
            });
    });*/

    describe("Create a Ckan Client and get info from the organization 'organization-test1' with success", function () {
        SyncClient = new CkanSyncClient(ckanTestEndpoint, ckanTestApiKey);

        it("Should show that the user has the organization 'organization-test1'", function (done) {
            SyncClient.ckanClient.action("organization_show",
                {
                    id: organizationMock.id
                },
                function (err, info) {
                    should.equal(err, null);
                    should.not.equal(info.info, null);
                    should.equal(info.result.title, organizationMock.title);
                    should.equal(info.result.description, organizationMock.description);
                    done();
                });
        });
    });

    describe("Create a package in the 'organization-test1", function () {
        it("Should create package 'testPackage1' in the organization 'organization-test1'", function (done) {
            SyncClient.ckanClient.action("package_create",
                {
                    name: packageMock.name,
                    notes: packageMock.notes,
                    author: packageMock.author,
                    owner_org: packageMock.owner_org
                },
                function (err, info) {
                    should.equal(err, null);
                    should.equal(info.result.author, packageMock.author);
                    should.equal(info.result.name, packageMock.name);
                    should.equal(info.result.notes, packageMock.notes);
                    should.equal(info.result.organization.name, packageMock.owner_org);
                    done();
                });
        });
    });

    /*describe("Upload two files in the package", function (done) {

    });

    describe("Update the package(delete a file and update a file)", function (done) {

    });*/
});
