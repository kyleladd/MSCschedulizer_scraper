var request = require('request');
var async = require('async');
var sjs = require('scraperjs');
var mysql      = require('mysql');
var genericfunctions = require('node_generic_functions');
var mysql_config = require('./mysql_config.js');
var fs = require('fs');
var exec = require('child_process').exec;
var stdio = require('stdio');
var options = stdio.getopt({
    'input': {description: 'Location of the file', default: 'sched_data.txt'},
    'test': {description: 'Use the test database connection?', default: 'false'}
});
console.dir(options);
var script_start_time = new Date().getTime();
var connection = mysql.createConnection(mysql_config.mySQLConfiguration(options.test));

var dbDepartments = [];
var dbCourseSections = [];
var dbCourses = [];
var dbMeetings = [];
var dbRequiredIdentifiers = [];
var dbTerms = [];

var departments = [];
var results = [];
async.series([
    function(callback){
        var array = fs.readFileSync(options.input).toString().split("\n");
        for(i in array) {
            var row = array[i].toString().split("\t");
            if(typeof row[1] != 'undefined'){
                row[9] = parseInt(row[9]);
                if(isNaN(row[9])){
                    row[9] = 0;
                }
                //Convert days to bits
                row[12] = genericfunctions.convertToBit(row[12]);
                row[13] = genericfunctions.convertToBit(row[13]);
                row[14] = genericfunctions.convertToBit(row[14]);
                row[15] = genericfunctions.convertToBit(row[15]);
                row[16] = genericfunctions.convertToBit(row[16]);
                results.push({Department:row[1].trim(),CourseNumber:row[2].trim(),CourseTitle:row[3].trim(),Weeks:row[4].trim(),CourseCRN:row[5].trim(),Section:row[6].trim(),Credits:row[9],CurrentEnrollment:row[10].trim(),MaxEnrollment:row[11].trim(),Monday:row[12],Tuesday:row[13],Wednesday:row[14],Thursday:row[15],Friday:row[16],StartTime:row[18].trim(),EndTime:row[19].trim(),Building:"".trim(),Room:"".trim(),Instructor:row[20].trim(),Identifier:row[7].trim(),RequiredIdentifiers:row[8].trim()});
            }
        }
        callback()
    },
    function(callback){
        clearDatabase(callback);
    },
    function(callback){
        console.log("DB Cleared/Reset");
        loadDatabase(results,callback);
    },
    function(callback){
        console.log("Everything is all loaded");
        // end connection
        connection.end();
        callback(null,"closed connection");
    }
    
],
// optional callback
function(err, results){
    var script_elapsed_time = (new Date().getTime() - script_start_time)/1000;
    console.log("Time to complete: " + script_elapsed_time + " seconds");
});

function clearDatabase(callback){
    var schema = fs.readFileSync('schema.sql').toString()
    connection.query(schema, function(err, result) {
    if (err){
      throw err;
    }
      callback(null,'reset db');
    });
}


function loadDatabase(results,callback){
    async.series([
    function(callback){
        console.log("loading departments");
        insertDepartments(results, callback);
    },
    function(callback){
        console.log("departments loaded");
        connection.query('SELECT id,abbreviation FROM departments', function(err, result) {
            if (err){
                throw err;
            }
            else{
                dbDepartments = result;
                callback(null,"departments selected");            
            }
        });
    },
    function(callback){
        console.log("departments selected");
        insertCourses(results, callback);
    },
    function(callback){
        console.log("courses loaded");
        connection.query('SELECT id,name,department_id,courseNumber FROM courses', function(err, result) {
            if (err){
                throw err;
            }
            else{
                dbCourses = result;
                callback(null,"courses selected");            
            }
        });
    },
    function(callback){
        console.log("courses selected");
        connection.query('SELECT id,abbreviation,name FROM course_terms', function(err, result) {
            if (err){
                throw err;
            }
            else{
                dbTerms = result;
                callback(null,"courses terms selected");            
            }
        });
    },
    function(callback){
        console.log("course terms selected");
        insertSections(results, callback);
    },

    function(callback){
        console.log("sections inserted");
        connection.query('SELECT id,name,course_id,courseCRN,instructor,currentEnrollment,maxEnrollment,credits,identifier FROM course_sections', function(err, result) {
            if (err){
                console.log("err here");
                throw err;
            }
            else{
                dbCourseSections = result;
                callback(null,"courses sections selected");            
            }
        });
    },
    function(callback){
        console.log("course sections selected");
        insertMeetings(results, callback);
    },
    function(callback){
        console.log("meetings loaded");
        connection.query('SELECT id,monday,tuesday,wednesday,thursday,friday,startTime,endTime,coursesection_id,building,room FROM meetings', function(err, result) {
            if (err){
                throw err;
            }
            else{
                dbMeetings = result;
                callback(null,"meetings selected");            
            }
        });
    },
    function(callback){
        console.log("meetings selected");
        insertRequiredIdentifiers(results, callback);
    },
    function(callback){
        console.log("Identifiers loaded");
        connection.query('SELECT id,identifier,section_id FROM required_identifiers', function(err, result) {
            if (err){
                throw err;
            }
            else{
                dbRequiredIdentifiers = result;
                callback(null,"Required Identifiers selected");            
            }
        });
    },
    function(callback){
        console.log("required identifiers selected");
        callback(null,"just console logging");
    }
    ],
    // optional callback
    function(err, results){
        callback(null,"loaded DB");
    });

    
}

function insertDepartments(results, callback){
    console.log("insert departments");
    var numRunningQueries = 0;
    var loadingDepartments = [];
    //For each department
    for(var c=0; c<results.length; c++){
        //for each course listing
            if(loadingDepartments.indexOf(results[c].Department)==-1){
                loadingDepartments.push(results[c].Department);
            }
    }
    for(var dept=0; dept<loadingDepartments.length; dept++){
        numRunningQueries++;
        connection.query('INSERT INTO departments (abbreviation) VALUES (\''+loadingDepartments[dept]+'\')', function(err, result) {
            if (err){
                throw err;
            }else{
                numRunningQueries--;
                if (numRunningQueries === 0) {
                    // finally, AFTER all callbacks did return:
                    callback(null,"departments loaded");            
                }
            }
        });
    }
}

function insertCourses(results, callback){
    var numRunningQueries = 0;
    var loadingCourses = [];
    //For each department
    for(var c=0; c<results.length; c++){
        //for each course listing
            if(genericfunctions.searchListDictionaries(loadingCourses,{Department:results[c].Department,CourseNumber:results[c].CourseNumber,CourseTitle:results[c].CourseTitle})==null){
                loadingCourses.push({Department: results[c].Department,CourseNumber: results[c].CourseNumber,CourseTitle:results[c].CourseTitle});
            }
    }
    for(var course=0; course<loadingCourses.length; course++){
        numRunningQueries++;
        var departmentkey = genericfunctions.searchListDictionaries(dbDepartments,{abbreviation:loadingCourses[course].Department});
        connection.query('INSERT INTO courses (name,department_id,courseNumber) VALUES (\''+loadingCourses[course].CourseTitle+'\','+departmentkey.id+',\''+loadingCourses[course].CourseNumber+'\')', function(err, result) {
            if (err){
                throw err;
            }else{
                numRunningQueries--;
                if (numRunningQueries === 0) {
                    // finally, AFTER all callbacks did return:
                    callback(null,"Courses loaded");            
                }
            }
        });
    }
}

function insertSections(results, callback){
    var numRunningQueries = 0;
    var loadingSections = [];
    //For each department
    for(var c=0; c<results.length; c++){
        //for each course listing
            //CourseCRN is unique
            if(genericfunctions.searchListDictionaries(loadingSections,{CourseCRN:results[c].CourseCRN})==null){
                var departmentkey = genericfunctions.searchListDictionaries(dbDepartments,{abbreviation:results[c].Department});
                var coursetermkey = genericfunctions.searchListDictionaries(dbTerms,{abbreviation:results[c].Weeks});
                loadingSections.push({Section:results[c].Section,CourseCRN:results[c].CourseCRN,Department:departmentkey.id,CourseNumber:results[c].CourseNumber,CourseTitle:results[c].CourseTitle,Instructor:results[c].Instructor,CurrentEnrollment:results[c].CurrentEnrollment,MaxEnrollment:results[c].MaxEnrollment,Credits:results[c].Credits,Identifier:results[c].Identifier,Weeks:coursetermkey});
            }
    }
    for(var section=0; section<loadingSections.length; section++){
        numRunningQueries++;
        var coursekey = genericfunctions.searchListDictionaries(dbCourses,{department_id:loadingSections[section].Department,courseNumber:loadingSections[section].CourseNumber,name:loadingSections[section].CourseTitle});
        connection.query('INSERT INTO course_sections (name,course_id,courseterm_id,courseCRN,instructor,currentEnrollment,maxEnrollment,credits,identifier) VALUES (\''+loadingSections[section].Section+'\','+coursekey.id+','+loadingSections[section].Weeks.id+',\''+loadingSections[section].CourseCRN+'\',\''+loadingSections[section].Instructor.replace(/'/g,"\\'")+'\','+loadingSections[section].CurrentEnrollment+','+loadingSections[section].MaxEnrollment+','+ loadingSections[section].Credits +',\''+ loadingSections[section].Identifier +'\')', function(err, result) {
            if (err){
                throw err;
            }else{
                numRunningQueries--;
                if (numRunningQueries === 0) {
                    // finally, AFTER all callbacks did return:
                    console.log("sections are now loaded");
                    callback(null,"Sections loaded");            
                }
            }
        });
    }
}

function insertMeetings(results, callback){
    var numRunningQueries = 0;
    var loadingMeetings = [];
    //For each department
    for(var c=0; c<results.length; c++){
        //for each course listing
        // for(var d=0; d<results[c].length; d++){
            //Explode and push for each meeting day?
            //For MySQL time datatype
            var startTime = genericfunctions.convertToInttime(results[c].StartTime);
            var endTime = genericfunctions.convertToInttime(results[c].EndTime);
            // startTime = startTime.toString();
            // endTime = endTime.toString();
            // startTime = startTime.substr(0, startTime.length -2) + ":" + startTime.substr(startTime.length -2);
            // endTime = endTime.substr(0, endTime.length -2) + ":" + endTime.substr(endTime.length -2);

            //If all days arent 0 and times are not empty - AKA online courses, then we have a meeting to add
            if(!((results[c].Monday == 0 && results[c].Tuesday == 0 && results[c].Wednesday == 0 && results[c].Thursday == 0 && results[c].Friday == 0) || (startTime == 0 && endTime == 0))){
                var coursesection = genericfunctions.searchListDictionaries(dbCourseSections,{courseCRN:results[c].CourseCRN});
                loadingMeetings.push({Section:coursesection.id,Monday:results[c].Monday,Tuesday:results[c].Tuesday,Wednesday:results[c].Wednesday,Thursday:results[c].Thursday,Friday:results[c].Friday,StartTime:startTime,EndTime:endTime,Building:results[c].Building,Room:results[c].Room});
            }
        // }
    }
    for(var meeting=0; meeting<loadingMeetings.length; meeting++){
        numRunningQueries++;
        connection.query('INSERT INTO meetings (monday,tuesday,wednesday,thursday,friday,startTime,endTime,coursesection_id,building,room) VALUES ('+loadingMeetings[meeting].Monday+','+loadingMeetings[meeting].Tuesday+','+loadingMeetings[meeting].Wednesday+','+loadingMeetings[meeting].Thursday+','+loadingMeetings[meeting].Friday+','+loadingMeetings[meeting].StartTime+','+loadingMeetings[meeting].EndTime+','+loadingMeetings[meeting].Section+',\''+loadingMeetings[meeting].Building.replace(/'/g,"\\'")+'\',\''+loadingMeetings[meeting].Room.replace(/'/g,"\\'")+'\')', function(err, result) {
            if (err){
                throw err;
            }else{
                numRunningQueries--;
                if (numRunningQueries === 0) {
                    // finally, AFTER all callbacks did return:
                    callback(null,"Meetings loaded");            
                }
            }
        });
    }
}

function insertRequiredIdentifiers(results, callback){
    var numRunningQueries = 0;
    var loadingRequiredIdentifiers = [];
    //For each department
    for(var c=0; c<results.length; c++){
        //for each course listing
        // for(var d=0; d<results[c].length; d++){

            //If there is a required Identifier
            if(results[c].RequiredIdentifiers != ""){
                var coursesection = genericfunctions.searchListDictionaries(dbCourseSections,{courseCRN:results[c].CourseCRN});
                var requiredIdentifiers = results[c].RequiredIdentifiers.split(":");
                for(var r=0; r<requiredIdentifiers.length; r++){
                    loadingRequiredIdentifiers.push({Section:coursesection.id,RequiredIdentifier:requiredIdentifiers[r]});
                }
            }
        // }
    }
    for(var requiredidentifier=0; requiredidentifier<loadingRequiredIdentifiers.length; requiredidentifier++){
        numRunningQueries++;
        connection.query('INSERT INTO required_identifiers (identifier,section_id) VALUES (\''+ loadingRequiredIdentifiers[requiredidentifier].RequiredIdentifier +'\','+ loadingRequiredIdentifiers[requiredidentifier].Section +')', function(err, result) {
            if (err){
                throw err;
            }else{
                numRunningQueries--;
                if (numRunningQueries === 0) {
                    // finally, AFTER all callbacks did return:
                    callback(null,"Required Identifiers loaded");            
                }
            }
        });
    }
}