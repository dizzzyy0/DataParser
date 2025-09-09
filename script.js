import pool from './db/db.js';
import fs from 'fs';
import JSONStream from 'JSONStream';

const BATCH_SIZE = 1000;
const FILE_PATH = './data/large_data.json';
const TABLE_NAME = 'users';

async function insertDataBatch(client, dataBatch) {
    const values = dataBatch.map(record => 
        `(${record.id},'${String(record.name).replace(/'/g, "''")}','${record.email}','${record.date_created}')`
    ).join(',');

    const query = `INSERT INTO ${TABLE_NAME} (id, name, email, date_created) VALUES ${values}`;

    try {
        await client.query(query);
    } catch (error) {
        console.error('Error inserting data:', error);
    }
};

async function processData() {
    const client = await pool.connect();

    try {
        await client.query(`CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            email VARCHAR(255),
            date_created TIMESTAMPTZ
        )`);

        await new Promise((resolve, reject) => {
            let batch = [];
            let totalInserted = 0;

            const fileStream = fs.createReadStream(FILE_PATH);
            const jsonStream = JSONStream.parse('*');

            fileStream.pipe(jsonStream)
                .on('data', async (record) => {
                    
                    batch.push(record);

                    if(batch.length >= BATCH_SIZE) {

                        jsonStream.pause();

                        await insertDataBatch(client, batch);

                        totalInserted += batch.length;

                        console.log(`Inserted ${totalInserted} records`);

                        batch = [];
                        jsonStream.resume();
                    }
                })
                .on('end', async () => {
                    if(batch.length > 0) {

                        await insertDataBatch(client, batch);

                        totalInserted += batch.length;

                        console.log(`Inserted ${totalInserted} records`);
                    }
                    resolve();
                })
                .on('error', (error) => {
                    console.error('Error reading JSON file:', error);
                    reject(error);
                });
        });

    } catch (error) {
        console.error('Error creating table:', error);
    } finally {
        client.release();
    }
};

processData().then(() => {
    console.log('Data processing completed.');
}).catch((error) => {
    console.error('Data processing failed:', error);
}).finally(async () => {
    await pool.end();
});
