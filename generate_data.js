import fs from 'fs';

const FILE_PATH = './data/large_data.json';
const NUM_RECORDS = 10000001;

if (!fs.existsSync('./data')) {
    fs.mkdirSync('./data');
};

const fileStream = fs.createWriteStream(FILE_PATH);

fileStream.write('[\n\t');

for( let i = 0; i < NUM_RECORDS; i++) {
    const record = {
        id: i,      
        name: `User_${i}`,
        email: `user_${i}@example.com`,
        date_created: new Date().toISOString()
    }

    const separator = i === NUM_RECORDS - 1 ? '' : ',\n\t';
    fileStream.write(JSON.stringify(record) + separator);

    if (i % 100000 === 0) {
        console.log(`Generated ${i} records`);
    }
};

fileStream.end('\n]');

fileStream.on('finish', () => {
    console.log(`Finished writing ${NUM_RECORDS} records to ${FILE_PATH}`);
});
