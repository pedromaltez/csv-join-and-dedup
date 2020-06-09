const csv = require('csv');
const fs = require('fs');
const dataFolder = './input/';

let output = new Set();

fs.readdir(dataFolder, (err, files) => {
  if (err)
    return console.log('Unable to scan directory: ' + err);
  files = files.filter(file => file.endsWith('.csv'))
  files.forEach( (file, idx) => {
    fs.createReadStream(`${dataFolder}${file}`)
      .pipe(csv.parse())
      .on('data', (row) => {
        // Include row ONLY if columns C and D are not empty or falsy
        if (row[2] && row[3]) {
          output.add(JSON.stringify(row))
        }
      })
      .on('end', () => {
        if (idx == files.length - 1) {
          output = Array.from(output).map(JSON.parse);
          csv.stringify(output, (err, output) => {
            if (err)
              return console.log('Unable to stringify output: ' + err);
            fs.writeFile('output.csv', output, (err) => {
              if (err)
                return console.log('Unable to save output.csv: ' + err);
              console.log('output.csv saved.');
            });
          })
        }
      console.log(`CSV file: ${file} successfully processed`);
      });
  });
});
