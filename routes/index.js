var express = require('express');
var router = express.Router();
const DB_TABLE = require('../DBConnection/tabelConstant');

const knex = require("../DBConnection/DBConnection");

// api to add mapping for new hospital
router.post('/mapping', async function (req, res, next) {
    const hospital_id = await knex(DB_TABLE.HOSPITALS).insert({name: req.body[0].hospitalName});

    const mappingToInsert = [];
    const patients = ["patient_id", "full_name", "sex", "mrn", "date_of_birth", "is_deceased", "state", "city", "zip_code", "address"];
    const treatments = ["treatment_id", "patient_id", "name", "status", "diagnoses", "start_date", "end_date", "days_cycle"];

    const payload = req.body;
    for (let i = 1; i < payload.length; i++) {
        let key = Object.keys(payload[i])[0];
        if (patients.indexOf(key) !== -1) {
            mappingToInsert.push({
                hospital_id: hospital_id[0],
                table_name: "patients",
                column_name: key,
                type: payload[i][key].type,
                value: payload[i][key].value
            });
        } else {
            mappingToInsert.push({
                hospital_id: hospital_id[0],
                table_name: "treatments",
                column_name: key == "patient_id_treatment" ? "patient_id" : key,
                type: payload[i][key].type,
                value: payload[i][key].value
            })
        }
    }

    const mapping = await knex(DB_TABLE.MAPPING_HOSPITALS).insert(mappingToInsert);

    res.json({
        res: "added successfully"
    });

})
    // api to get the logs for the uploaded files
    .get('/hospitals/:hospital_id/logs', async (req, res, next) => {
        const hospital_id = req.params.hospital_id
        const logs = await knex(DB_TABLE.HOSPITAL_LOG).select('*').where('hospital_id', hospital_id);
        res.json({
            result: logs
        })
    })
module.exports = router;
