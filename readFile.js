var fs = require('fs');
const knex = require("./DBConnection/DBConnection");
const moment = require("moment");
const DB_TABLE = require('./DBConnection/tabelConstant');

const tableKeys = {
    patients: "patient_id",
    treatments: "treatment_id"
};

const storeFile = async (path, tableName, hospital_id) => {

    const hospitalMappingTable = await getHospitalMappingTable(tableName, hospital_id);

    const patientIds = await getPatientIds();


    const stream = fs.createReadStream(path);
    let objToInsert = {};
    let unprocessed = '';
    const importedFileAsJson = [];
    let mappedDataAsJson = [];
    const logFile = [];
    let keys=[];
    let rowCount = 0 ;
    let flag = false;
    stream.on("data", async (chunk) => {
        let chunkString = unprocessed + chunk.toString();
        unprocessed = '';


        let startIndex = 0;
        for(let ch = startIndex; ch < chunk.length; ch++) {
            objToInsert = {};

            if (chunkString[ch] === "\n") {
                rowCount++;

                const line = chunkString.slice(startIndex, ch);
                if (!flag) {
                    keys = line.split(',')
                    flag = true;
                }
                else {
                    const columns = line.split(',');
                    const rowAsJson = {};
                    columns.forEach((colValue, index) => {
                        rowAsJson[keys[index].trim()] = colValue;
                    })

                    // convert each line in csv to json
                    importedFileAsJson.push(rowAsJson)

                    const row = {};

                    // apply our mapping to get useful and readable data from us
                    hospitalMappingTable.forEach((hospitalMapItem) => {
                        const ourColName = hospitalMapItem.column_name;

                        if (hospitalMapItem.type === 'COLUMN') {
                            row[ourColName] = rowAsJson[hospitalMapItem.value]
                        }
                        else if (hospitalMapItem.type === 'FUNCTION') {
                            eval(hospitalMapItem.value);
                            row[ourColName] = myFunc(rowAsJson);
                        }
                    })


                    // log Errors
                    if (tableName == "treatments" && patientIds.indexOf(parseInt(row['patient_id'])) == -1) {
                        logFile.push({
                            row: "the treatment row number " + rowCount + " have patient id doesn't exist in our Database",
                            rawContent: rowAsJson
                        });
                    } else if (tableName == "treatments" && row["treatment_id"] == "") {
                        logFile.push({
                            row: "the treatment row number " + rowCount + " doesn't have treatment id",
                            rawContent: rowAsJson
                        });
                    } else if (tableName == "patients" && row['patient_id'] == "") {
                        logFile.push({
                            row: "the patient row number " + rowCount + " doesn't have patient id",
                            rawContent: rowAsJson
                        });
                    } else {
                        // store full record and assign record to hospital
                        row['hospital_id'] = hospital_id;
                        row['full_record'] = rowAsJson;

                        // store data we want to insert
                        mappedDataAsJson.push(row);

                        if (mappedDataAsJson.length == 1000) {
                            // insert the 1000 record as bulk insert
                            await knex(tableName).insert(mappedDataAsJson).onConflict(tableKeys[tableName]).merge();
                            mappedDataAsJson = [];
                        }
                    }
                }

                startIndex = ch + 1;
            }
        }


        // console.log(importedFileAsJson)
        // console.log(mappedDataAsJson)

        // insert the rest data as bulk insert
        await knex(tableName).insert(mappedDataAsJson).onConflict(tableKeys[tableName]).merge();


        // insert logs of the failed record
        await knex(DB_TABLE.HOSPITAL_LOG).insert({
            hospital_id: hospital_id,
            logs: JSON.stringify(logFile) ,
            created_at: moment().format('YYYY-MM-DD HH:mm:ss')
        });
    });

    stream.on("end", () => {
        console.log("Stream finished reading")
    })
}

const getHospitalMappingTable = async (tableName, hospital_id) => {
    let schema = await knex(DB_TABLE.MAPPING_HOSPITALS).select('*')
        .where('table_name', tableName)
        .where('hospital_id', hospital_id);
    return schema;
}

const getPatientIds = async () => {
  let patientIds = await knex(DB_TABLE.PATIENTS).select('patient_id');
  const ids = patientIds.map((id)=> id.patient_id)
  return ids;
}

storeFile('C:\\Users\\atome\\Documents\\Software Engineer\\hospital_1_Treatment.csv', DB_TABLE.TREATMENTS, 1);

