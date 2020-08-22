const fs = require('fs');
const glob = require('glob');
const path = require('path');

if (!process.argv[2]) {
    console.error();
    console.error('Usage: node upgrade.js <openaddresses path>');
    console.error();
    return;
}

main();

async function main() {
    try {
        await convert();
    } catch(err) {
        console.error(err);
    }
}

async function convert() {
    return new Promise((resolve, reject) => {
        console.error('ok - beginning conversion');
        glob(path.resolve(process.argv[2], 'sources/**/*.json'), {
            nodir: false
        }, async (err, files) => {
            if (err) throw err;

            for (const file of files) {
                const psd = JSON.parse(fs.readFileSync(file))

                if (psd.schema === 2) continue;

                if (psd.coverage.city) {
                    name = 'city';
                } else if (psd.coverage.town) {
                    name = 'town';
                } else if (psd.coverage.county) {
                    name = 'county';
                } else if (psd.coverage.district) {
                    name = 'district';
                } else if (psd.coverage.region) {
                    name = 'region';
                } else if (psd.coverage.province) {
                    name = 'province';
                } else if (psd.coverage.state) {
                    name = 'state';
                } else if (psd.coverage.country) {
                    name = 'country';
                }

                psd.schema = 2;
                psd.layers = {
                    addresses: [{
                        name: name
                    }]
                }

                for (const key of Object.keys(psd)) {
                    if (['schema', 'layers', 'coverage'].includes(key)) continue;
                    psd.layers.addresses[0][key] = psd[key]
                    delete psd[key];
                }

                fs.writeFileSync(file, JSON.stringify(psd, null, 4));
            }

            return resolve();
        });
    });
}
