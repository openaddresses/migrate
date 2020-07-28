const fs = require('fs');
const glob = require('glob');
const path = require('path');

if (!process.argv[2]) {
    console.error();
    console.error('Usage: node cache.js <openaddresses path>');
    console.error();
}

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
})
