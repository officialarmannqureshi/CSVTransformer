const { parseQuery } = require("./queryParser");
const readCSV = require("./csvReader");
// Helper functions for different JOIN types
function performInnerJoin(data, joinData, joinCondition, fields, table) {
  // Logic for INNER JOIN
  data = data.flatMap((mainRow) => {
    return joinData
      .filter((joinRow) => {
        const mainVal = mainRow[joinCondition.left.split(".")[1]];
        const joinVal = joinRow[joinCondition.right.split(".")[1]];
        return mainVal === joinVal;
      })
      .map((joinRow) => {
        return fields.reduce((acc, field) => {
          const [tableName, fieldName] = field.split(".");
          acc[field] =
            tableName === table ? mainRow[fieldName] : joinRow[fieldName];
          return acc;
        }, {});
      });
  });
  return data;
}

function performLeftJoin(data, joinData, joinCondition, fields, table,joinTable) {
  // Logic for LEFT JOIN
  // ...
  const data1 = data.flatMap((mainRow) => {
    return joinData
      .filter((joinRow) => {
        const mainVal = mainRow[joinCondition.left.split(".")[1]];
        const joinVal = joinRow[joinCondition.right.split(".")[1]];
        return mainVal === joinVal;
      })
      .map((joinRow) => {
        return fields.reduce((acc, field) => {
          const [tableName, fieldName] = field.split(".");
          acc[field] =
            tableName === table ? mainRow[fieldName] : joinRow[fieldName];
          return acc;
        }, {});
      });
  });
  const joinFields=[];
  for (let field of fields){
    if(field.split('.')[0]===joinTable){
      joinFields.push(field.split('.')[1])
    }
  }
  const mainFields=[];
  for (let field of fields){
    
    if(field.split('.')[0]===table){
      mainFields.push(field.split('.')[1])
    }
  }


  let result = [];
  for (const mainRow of data) {
    const data2 = {};
    let foundMatch = false;
    for (const joinRow of joinData) {

      if (mainRow['id'] === joinRow['student_id']) {
        foundMatch = true;
        break;
      }
    }
    if (!foundMatch) {
      for (const field of fields) {
        let fieldDetail = field.split('.');
        if (fieldDetail[0] === joinTable) {
          data2[fieldDetail[0]+'.'+fieldDetail[1]] = null;
        } else {
          data2[fieldDetail[0]+'.'+fieldDetail[1]] = mainRow[fieldDetail[1]];
        }
      }
      result.push(data2);
    }
    
  }
 
  if(result){
    
    data1.push(...result);
    
  }
  
  return data1;
  
}

function performRightJoin(data, joinData, joinCondition, fields, table,joinTable) {
  // Logic for RIGHT JOIN
  const data1 = data.flatMap((mainRow) => {
    return joinData
      .filter((joinRow) => {
        const mainVal = mainRow[joinCondition.left.split(".")[1]];
        const joinVal = joinRow[joinCondition.right.split(".")[1]];
        return mainVal === joinVal;
      })
      .map((joinRow) => {
        return fields.reduce((acc, field) => {
          const [tableName, fieldName] = field.split(".");
          acc[field] =
            tableName === table ? mainRow[fieldName] : joinRow[fieldName];
          return acc;
        }, {});
      });
  });
  const joinFields=[];
  for (let field of fields){
    if(field.split('.')[0]===joinTable){
      joinFields.push(field.split('.')[1])
    }
  }
  const mainFields=[];
  for (let field of fields){
    
    if(field.split('.')[0]===table){
      mainFields.push(field.split('.')[1])
    }
  }


  let result = [];
  for (const joinRow of joinData) {
    const data2 = {};
    let foundMatch = false;
    for (const mainRow of data) {

      if (mainRow['id'] === joinRow['student_id']) {
        foundMatch = true;
        break;
      }
    }
    if (!foundMatch) {
      for (const field of fields) {
        let fieldDetail = field.split('.');
        if (fieldDetail[0] === table) {
          data2[fieldDetail[0]+'.'+fieldDetail[1]] = null;
        } else {
          data2[fieldDetail[0]+'.'+fieldDetail[1]] = joinRow[fieldDetail[1]];
        }
      }
      
      result.push(data2);
    }
    
  }

  if(result){
    
    data1.push(...result);
    
  }
  
  return data1;
}
async function executeSELECTQuery(query) {
  const { fields, table, whereClauses,joinType, joinTable, joinCondition,groupByFields} =
    parseQuery(query);
   

  let data = await readCSV(`${table}.csv`);

  
  console.log(parseQuery(query))
  if(groupByFields){
    //Group By Fields
    
    data= applyGroupBy(data,groupByFields,fields)
  }
  // Logic for applying JOINs
  if (joinTable && joinCondition) {
    const joinData = await readCSV(`${joinTable}.csv`);
    switch (joinType.toUpperCase()) {
      case "INNER":
        data = performInnerJoin(data, joinData, joinCondition, fields, table,joinTable);
        break;
      case "LEFT":
        data = performLeftJoin(data, joinData, joinCondition, fields, table,joinTable);
        break;
      case "RIGHT":
        data = performRightJoin(data, joinData, joinCondition, fields, table,joinTable);
        break;
      // Handle default case or unsupported JOIN types
      default:
        throw new Error("Invalid Join Statement");
    }
  }

  // GroupBy logic
  



  // Filtering based on WHERE clause
  const filteredData =
    whereClauses.length > 0
      ? data.filter((row) =>
          whereClauses.every((clause) => evaluateCondition(row, clause))
        )
      : data;

  // Selecting the specified fields
  return filteredData.map((row) => {
    const selectedRow = {};
    fields.forEach((field) => {
      selectedRow[field] = row[field];
    });
    return selectedRow;
  });
}
// Helper function to apply GROUP BY and aggregate functions
function applyGroupBy(data, groupByFields, fields) {
  // Implement logic to group data and calculate aggregates
  // ...

  const aggregateRegrex=/^(\w*).(\w*)./i
  const aggregateFunction={}
  for ( let field of fields){
    if(field.includes('COUNT') || field.includes('MAX') || field.includes('AVG') || field.includes('MAX') ||field.includes('MIN'))
    {
      const result=field.match(aggregateRegrex);
      aggregateFunction[[result[2]]]=result[1]
    }
    // { id: 'COUNT' }
  }
  console.log(aggregateFunction);
  return data;
}

function evaluateCondition(row, clause) {
  const { field, operator, value } = clause;
  switch (operator) {
    case "=":
      return row[field] === value;
    case "!=":
      return row[field] !== value;
    case ">":
      return row[field] > value;
    case "<":
      return row[field] < value;
    case ">=":
      return row[field] >= value;
    case "<=":
      return row[field] <= value;
    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
}
// async function main(){

//     const input = process.argv[2];
//     const query = input;
//     const res= await executeSELECTQuery(query);
//     console.log("Result:",res);
// }
// main();
/*
List of errors:
* field -  Choosing all fields option not there
*/

module.exports = executeSELECTQuery;
