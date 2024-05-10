function parseQuery(query) {
    // First, let's trim the query to remove any leading/trailing whitespaces
    query = query.trim();

    // Initialize variables for different parts of the query
    let selectPart, fromPart;

    // Split the query at the WHERE clause if it exists
    const whereSplit = query.split(/\sWHERE\s/i);
    query = whereSplit[0]; // Everything before WHERE clause

    // WHERE clause is the second part after splitting, if it exists
    const whereClause = whereSplit.length > 1 ? whereSplit[1].trim() : null;

    // Split the remaining query at the JOIN clause if it exists
    const joinSplit = query.split(/\s(INNER|LEFT|RIGHT) JOIN\s/i);
    let joinPart = null;

    if (joinSplit.length > 1) {
    joinPart = joinSplit[1] + ' ' + joinSplit[2].trim();
    }

    
    
    selectPart = joinSplit[0].trim(); // Everything before JOIN clause

    if(selectPart.includes('GROUP')){
        selectPart=selectPart.split(/GROUP BY/)[0]
    }
    // JOIN clause is the second part after splitting, if it exists
    const groupByRegex = /\sGROUP BY\s(.+)/i;
    const groupByMatch = query.match(groupByRegex);
    
    
    let groupByFields = null;
    if (groupByMatch) {
        groupByFields = groupByMatch[1].split(',').map(field => field.trim());
    }

    
    // Parse the SELECT part
    const selectRegex = /^SELECT\s(.+?)\sFROM\s(.+)?/i;
    const selectMatch = selectPart.match(selectRegex);
    if (!selectMatch) {
        throw new Error('Invalid SELECT format');
    }
    
    const [, fields, table] = selectMatch;
    
    // Parse the JOIN part if it exists
    let joinTable = null, joinCondition = null, joinType = null;
    
    if (joinPart) {
        // const joinRegex = /^(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
        // const joinMatch = joinPart.match(joinRegex);
        // if (!joinMatch) {
        //     throw new Error('Invalid JOIN format');
        // }

        // joinTable = joinMatch[1].trim();
        // joinCondition = {
        //     left: joinMatch[2].trim(),
        //     right: joinMatch[3].trim()
        // };
       
        
        const parsedJoin=parseJoinClause(joinPart);
        if(!parsedJoin){
            throw new Error('Invalid JOIN format')
        }
        
        joinType=parsedJoin.joinType;
        joinTable=parsedJoin.joinTable;
        joinCondition=parsedJoin.joinCondition;
        

    }

    // Parse the WHERE part if it exists
    let whereClauses = [];
    if (whereClause) {
        whereClauses = parseWhereClause(whereClause);
    }
    

    

    return {
        fields: fields.split(',').map(field => field.trim()),
        table: table.trim(),
        whereClauses,
        joinType,
        joinTable,
        joinCondition,
        groupByFields
    };
}
function checkQuery(query){
    const selectRegex = /SELECT (.+?) FROM (.+?)(?: WHERE (.*))?$/i;
    
    let isValid=false;
    isValid= selectRegex.ignoreCase? true : false
    return isValid;
}
function parseWhereClause(whereString) {
    try{
        const conditionRegex = /(.*?)(=|!=|>|<|>=|<=)(.*)/;
        return whereString.split(/ AND | OR /i).map(conditionString => {
            
            const match = conditionString.match(conditionRegex);

            if (match) {
                const [, field, operator, value] = match;
                if(field && operator && value )
                return { field: field.trim(), operator, value: value.trim() };
                else
                throw new Error('Missing any of these field,operator & value');
            }else{
                throw new Error('Invalid WHERE clause format');
            }
            
        });
    }
    catch(err){
        throw err;
    }
}
function parseJoinClause(query) {
    const joinRegex = /^(INNER|LEFT|RIGHT)\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
  
    let joinMatch = query.match(joinRegex);
   
    if (joinMatch) {
        
        return {
            joinType: joinMatch[1].trim(),
            joinTable: joinMatch[2].trim(),
            joinCondition: {
                left: joinMatch[3].trim(),
                right: joinMatch[4].trim()  
            }
        };
    }

    return {
        joinType: null,
        joinTable: null,
        joinCondition: null
    };
}
module.exports = {parseQuery,checkQuery};