const readCSV = require("../src/csvReader");
const { checkQuery, parseQuery } = require("../src/queryParser");

test("Read CSV File", async () => {
  const data = await readCSV("./student.csv");

  expect(data.length).toBeGreaterThan(0);
  expect(data.length).toBe(3);
  expect(data[0].name).toBe("John");
  expect(data[0].age).toBe("30"); //ignore the string type here, we will fix this later
});

test("Case Sensitive Query", () => {
  const query = "Select id,name from student where age = 25";
  const result = checkQuery(query);

  expect(result).toBe(true);
});
test("Parse SQL Query", () => {
  const query = "SELECT id,name FROM student WHERE age = 25";
  const parsed = parseQuery(query);

  expect(parsed).toEqual({
    fields: ["id", "name"],
    table: "student",
    whereClauses: [
      {
        field: "age",
        operator: "=",
        value: "25",
      },
    ],
    joinCondition: null,
    joinTable: null,
    joinType: null,
    groupByFields:null
  });
});

const executeSELECTQuery = require("../src/index");

test("Execute SQL Query with WHERE Clause", async () => {
  const query = "SELECT id, name FROM student WHERE age = 25";
  const result = await executeSELECTQuery(query);

  expect(result.length).toBe(1);
  expect(result[0]).toHaveProperty("id");
  expect(result[0]).toHaveProperty("name");
  expect(result[0].id).toBe("2");
});

test("Parse SQL Query with Multiple WHERE Clauses", () => {
  const query = "SELECT id, name FROM student WHERE age = 30 AND name = John";
  const parsed = parseQuery(query);
  expect(parsed).toEqual({
    fields: ["id", "name"],
    table: "student",
    whereClauses: [
      {
        field: "age",
        operator: "=",
        value: "30",
      },
      {
        field: "name",
        operator: "=",
        value: "John",
      },
    ],
    joinCondition: null,
    joinTable: null,
    joinType: null,
    groupByFields:null
  });
});

test("Execute SQL Query with Multiple WHERE Clause", async () => {
  const query = "SELECT id, name FROM student WHERE age = 30 AND name = John";
  const result = await executeSELECTQuery(query);
  expect(result.length).toBe(1);
  expect(result[0]).toEqual({ id: "1", name: "John" });
});
test("Execute SQL Query with Greater Than", async () => {
  const queryWithGT = "SELECT id FROM student WHERE age > 22";
  const result = await executeSELECTQuery(queryWithGT);
  expect(result.length).toEqual(2);
  expect(result[0]).toHaveProperty("id");
});

test("Execute SQL Query with Not Equal to", async () => {
  const queryWithGT = "SELECT name FROM student WHERE age != 25";
  const result = await executeSELECTQuery(queryWithGT);
  expect(result.length).toEqual(2);
  expect(result[0]).toHaveProperty("name");
});
test("Invalid Comparison Operator", async () => {
  const queryWithInvalidOperator = "SELECT * FROM student WHERE age $ 30";
  await expect(executeSELECTQuery(queryWithInvalidOperator)).rejects.toThrow(
    "Invalid WHERE clause format"
  );
});
test("Invalid WHERE Clause Format", async () => {
  const queryWithInvalidWhere = "SELECT * FROM student WHERE age 30";
  await expect(executeSELECTQuery(queryWithInvalidWhere)).rejects.toThrow(
    "Invalid WHERE clause format"
  );
});
test("Missing Field in Condition", async () => {
  const queryWithMissingField = "SELECT * FROM student WHERE > 30";
  await expect(executeSELECTQuery(queryWithMissingField)).rejects.toThrow(
    "Missing any of these field,operator & value"
  );
});

test("Execute SQL Query with INNER JOIN", async () => {
  const query =
    "SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id";
  const result = await executeSELECTQuery(query);

  expect(result).toEqual([
    { "enrollment.course": "Physics", "student.name": "John" },
    { "enrollment.course": "Chemistry", "student.name": "Jane" },
  ]);
});
test("Execute SQL Query with INNER JOIN and WHERE Clause", async () => {
  const query =
    "SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id WHERE student.name = John";
  const result = await executeSELECTQuery(query);

  expect(result).toEqual([
    { "enrollment.course": "Physics", "student.name": "John" },
  ]);
});
test("Parse SQL Query with INNER JOIN", async () => {
  const query =
    "SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id";
  const result = await parseQuery(query);

  expect(result).toEqual({
    fields: ["student.name", "enrollment.course"],
    table: "student",
    whereClauses: [],
    joinTable: "enrollment",
    joinType: "INNER",
    joinCondition: { left: "student.id", right: "enrollment.student_id" },
    groupByFields:null
  });
});
test("Parse SQL Query with INNER JOIN and WHERE Clause", async () => {
  const query =
    "SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id WHERE student.name = John";
  const result = await parseQuery(query);

  expect(result).toEqual({
    fields: ["student.name", "enrollment.course"],
    joinCondition: { left: "student.id", right: "enrollment.student_id" },
    joinTable: "enrollment",
    joinType: "INNER",
    table: "student",
    whereClauses: [{ field: "student.name", operator: "=", value: "John" }],
    groupByFields:null
  });
});

test("Execute SQL Query with LEFT JOIN", async () => {
  const query =
    "SELECT student.name, enrollment.course FROM student LEFT JOIN enrollment ON student.id=enrollment.student_id";
  const result = await executeSELECTQuery(query);
  expect(result).toEqual([
    { "enrollment.course": "Physics", "student.name": "John" },
    { "enrollment.course": "Chemistry", "student.name": "Jane" },
    { "enrollment.course": null, "student.name": "Bob" },
  ]);
});

test("Execute SQL Query with RIGHT JOIN", async () => {
  const query =
    "SELECT student.name, enrollment.course FROM student RIGHT JOIN enrollment ON student.id=enrollment.student_id";
  const result = await executeSELECTQuery(query);
  expect(result).toEqual([
    { "enrollment.course": "Physics", "student.name": "John" },
    { "enrollment.course": "Chemistry", "student.name": "Jane" },
    { "enrollment.course": "Computers", "student.name": null },
  ]);

});

test('Parse Groupby queries with aggregate functions',async ()=>{
  const query="SELECT COUNT(id),name FROM student GROUP BY name,id";
  const result = await parseQuery(query);
  expect(result).toEqual({
    fields: [ 'COUNT(id)', 'name' ],
      table: 'student',
      whereClauses: [],
      joinType: null,
      joinTable: null,
      joinCondition: null,
      groupByFields: ['name','id']
  });
})
test('Execute Groupby queries with aggregate functions',async ()=>{
  const query="SELECT COUNT(id),name FROM student GROUP BY name,id";
  const result = await executeSELECTQuery(query);
  expect(result).toEqual([]);
})

// test('Checks case-insensitivity', async () => {
//     const query = 'Select * from student where age = 25';
//     const result = await case_sensitive(query);
//     expect(result.length).toBe(1);
//     expect(result[0]).toHaveProperty('id');
//     expect(result[0]).toHaveProperty('name');
//     expect(result[0]).toHaveProperty('age');
//     expect(result[0].id).toBe('2');
// });
