const fs = require('fs');
const glob = require('glob');
const path = require('path');
const { Pool } = require('pg');
const copyFrom = require('pg-copy-streams').from

const pool = new Pool({
    host: 'localhost',
    user: 'postgres',
    database: 'oalogs'
});

if (!process.argv[2] || !process.argv[3]) {
    console.error();
    console.error('Usage: node cache.js <openaddresses path> <s3 inventory.csv>');
    console.error();
    console.error('Download s3 inventory from s3://logs.openaddresses.io/s3inventories/data.openaddresses.io/all-objects-in-data/data/');
    console.error();
    return;
}

main();

async function main() {
    try {
        await logs();
        await logs_optimize();
        await convert();
    } catch(err) {
        console.error(err);
    }
}

async function logs() {
    return new Promise((resolve, reject) => {
        pool.connect(async (err, client, done) => {
            if (err) return reject(err);

            try {
                await client.query(`
                    CREATE TABLE IF NOT EXISTS logs (
                        bucket      TEXT,
                        file        TEXT,
                        size        BIGINT,
                        modified    TIMESTAMP,
                        hash        TEXT,
                        zone        TEXT
                    );
                `);

                await client.query(`
                    CREATE TABLE IF NOT EXISTS parsed (
                        file        TEXT,
                        run         BIGINT,
                        source      TEXT,
                        modified    TIMESTAMP
                    );
                `);

                await client.query(`
                    DELETE FROM logs;
                `);

                await client.query(`
                    DELETE FROM parsed;
                `);

                await client.query(`
                    DROP INDEX IF EXISTS log_source;
                `);

                glob(path.resolve(process.argv[3], '*.csv'), {
                    nodir: false
                }, async (err, files) => {
                    if (err) return reject(err);

                    for (const file of files) {
                        console.error(`ok - importing ${file}`);
                        try {
                            await single_log(client, file);
                        } catch (err) {
                            return reject(new Error(err));
                        }
                    }

                    client.end();
                    return resolve();
                });
            } catch (err) {
                return reject(err);
            }
        });
    });
}

async function single_log(client, file) {
    return new Promise((resolve, reject) => {
        const stream = client.query(copyFrom(`
            COPY logs (
                bucket,
                file,
                size,
                modified,
                hash,
                zone
            ) FROM STDIN DELIMITER ',' CSV;
        `));
        const fileStream = fs.createReadStream(file)
        fileStream.on('error', reject);
        stream.on('error', reject);
        stream.on('finish', () => {
            return resolve();
        });
        fileStream.pipe(stream);
    });
}

async function logs_optimize() {
    return new Promise((resolve, reject) => {
        console.error('ok - optimizing logs');
        pool.connect(async (err, client, done) => {
            if (err) return reject(err);

            try {
                console.error('ok - populating parsed');
                await client.query(`
                    INSERT INTO parsed (
                        file,
                        run,
                        source,
                        modified
                    ) (
                        SELECT
                            file,
                            regexp_replace(replace(file, 'runs/', ''), '/.*', '')::BIGINT,
                            replace(replace(regexp_replace(file, 'runs/([0-9]+)/', ''), '/', '-'), '.zip', '') AS source,
                            modified
                        FROM
                            logs
                        WHERE
                            file ~ 'runs/[0-9]+/.*zip'
                            AND file NOT LIKE 'runs/%/cache.zip'
                    )
                `);

                console.error('ok - creating source index');
                await client.query(`
                    CREATE INDEX log_source
                        ON parsed (source);
                `);

                client.end()
                console.error('ok - optimization complete');
                return resolve();

            } catch (err) {
                return reject(err);
            }
        });
    });
}

async function lookup(client, source) {
    return new Promise((resolve, reject) => {
        client.query(`
            SELECT
                *
            FROM
                parsed
            WHERE
                source = ${source}
            ORDER BY
                modified DESC
        `, (err, pgres) => {
            if (err) return reject(err);

            console.error(pgres);

            return resolve();
        });
    });
}

async function convert() {
    return new Promise((resolve, reject) => {
        console.error('ok - beginning conversion');
        pool.connect((err, client, done) => {
            if (err) return reject(err);

            glob(path.resolve(process.argv[2], 'sources/**/*.json'), {
                nodir: false
            }, (err, files) => {
                if (err) throw err;

                for (const file of files) {
                    const psd = JSON.parse(fs.readFileSync(file))

                    if (psd.schema === 2) continue;

                    if (psd.coverage.city) name = 'city';
                    if (psd.coverage.town) name = 'town';
                    if (psd.coverage.county) name = 'county';
                    if (psd.coverage.district) name = 'district';
                    if (psd.coverage.region) name = 'region';
                    if (psd.coverage.province) name = 'province';
                    if (psd.coverage.state) name = 'state';
                    if (psd.coverage.country) name = 'country';

                    psd.schema = 2;
                    psd.layers = {
                        addresses: [{
                            name: name
                        }]
                    }

                    for (const key of Object.keys(psd)) {
                        if (['coverage', 'layers', 'schema'].includes(key)) continue;

                        psd.layers.addresses[0][key] = psd[key];
                        delete psd[key];
                    }

                    fs.writeFileSync(file, JSON.stringify(psd, null, 4));
                }

                client.end();
                return resolve();
            });
        });
    });
}
