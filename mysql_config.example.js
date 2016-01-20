var genericfunctions = require('node_generic_functions');
module.exports = {
  mySQLConfiguration: function (isTest) {
    // Multiple Statements is for resetting the database from schema sql file
    if(genericfunctions.toBoolean(isTest)===true){
        return {
            host:"",
            database:"",
            user: "",
            password: "",
            multipleStatements: true
        };
    }
    return {
        host:"",
        database:"",
        user: "",
        password: "",
        multipleStatements: true
    };
  }
};