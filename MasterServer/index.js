const express = require('express');
const fs = require('fs');
const multer = require('multer');
const path = require('path');
const axios = require('axios');

const app = express();
const PORT = 4000;
const SERVER_2_URL = 'http://localhost:5000/upload'; // Server 2 URL
const SERVER_3_URL = 'http://localhost:5001/upload'; // Server 3 URL
const SERVER_4_URL = 'http://localhost:5002/upload'; // Server 4 URL

const storage = multer.memoryStorage(); // Store file in memory

const upload = multer({ storage: storage });

const UPLOADS_DIR = path.join(__dirname, 'uploads');

const mysql = require('mysql2');

// Create a connection to the MySQL database
const connection = mysql.createConnection({
    host: 'localhost', // Your MySQL host (usually 'localhost')
    port: 3306, // Your MySQL port (default is 3306)
    user: 'root', // Your MySQL username
    password: '', // Your MySQL password
    database: 'hussainassignment' // Your MySQL database name
});

// Connect to the database
connection.connect((err) => {
    if (err) {
        console.error('Error connecting to MySQL database: ' + err.stack);
        return;
    }
    console.log('Connected to MySQL database.');
 
    // Check if the table exists
    connection.query("SHOW TABLES LIKE 'chunkMetaData'", (err, results) => {
        if (err) {
            console.error('Error checking if table exists: ' + err.stack);
            connection.end();
            return;
        }
 
        if (results.length === 0) {
            // Table does not exist, create it
            const createTableQuery = `
                CREATE TABLE chunkMetaData (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    file VARCHAR(255),
                    chunk INT,
                    server VARCHAR(255)
                )
            `;
 
            connection.query(createTableQuery, (err, results) => {
                if (err) {
                    console.error('Error creating table: ' + err.stack);
                    connection.end();
                    return;
                }
                console.log('Table created successfully.');
            });
        } else {
            console.log('Table already exists.');
        }
 
        
    });
 });


// Set up a route for file upload
app.post('/upload', upload.single('file'), async (req, res) => {
  const file = req.file;
  if (!file) {
    return res.status(400).json({ error: 'No file uploaded' });
  }

  const fileName = file.originalname;
  const fileNameWithoutExtension = path.parse(fileName).name;
  const fileExtension = path.extname(fileName);
  const fileSize = file.size;
  const fileBuffer = file.buffer;
  const chunkSize = Math.ceil(fileSize / 4);

  // Function to create the uploads directory if it doesn't exist
  const createUploadsDirectory = () => {
    const uploadDir = path.join(__dirname, 'uploads');
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir);
    }
  };

  // Check if uploads directory exists or create it
  createUploadsDirectory();

  // Divide the file into chunks
  const chunks = [];
  for (let i = 0; i < 4; i++) {
    const start = i * chunkSize;
    const end = Math.min(start + chunkSize, fileSize);
    const chunkData = fileBuffer.slice(start, end);
    chunks.push(chunkData);
  }

  // Save the first two chunks locally and send to Server 3
  chunks.slice(0, 2).forEach(async (chunk, index) => {
    const chunkFileName = `${fileNameWithoutExtension}_${index}${fileExtension}`; // Use the same extension
    const chunkFilePath = path.join(__dirname, 'uploads', chunkFileName);

    // Save locally
    fs.writeFile(chunkFilePath, chunk,   (err) => {
      if (err) {
        console.error(`Error saving local chunk ${index}:`, err);
      } else {
        console.log(`Local chunk ${index} saved successfully`);
         // Insert record into the database
         try {
             connection.execute('INSERT INTO chunkMetaData (file, chunk, server) VALUES (?, ?, ?)', [fileNameWithoutExtension, index, 'Master']);
            console.log(`Record for local chunk ${index} inserted into the database`);
          } catch (error) {
            console.error(`Error inserting record for local chunk ${index} into the database:`, error);
          }
      }
    });

    // Send to Server 3
    try {
      await axios.post(SERVER_3_URL, {
        fileName: chunkFileName,
        chunkData: chunk.toString('base64'), // Convert chunk data to base64
      }).then(()=>{
        // Insert record into the database
        try {
            connection.execute('INSERT INTO chunkMetaData (file, chunk, server) VALUES (?, ?, ?)', [fileNameWithoutExtension, index, '3']);
            console.log(`Record for local chunk ${index} inserted into the database`);
        } catch (error) {
            console.error(`Error inserting record for local chunk ${index} into the database:`, error);
        }
      });;
      console.log(`Chunk ${index} sent to Server 3 successfully`);
    } catch (error) {
      console.error(`Error sending chunk ${index} to Server 3:`, error.message);
    }
  });

  // Send the last two chunks to Server 2 and Server 4
  const chunksToSend = chunks.slice(2);
  try {
    await Promise.all(chunksToSend.map(async (chunk, index) => {
      const chunkFileName = `${fileNameWithoutExtension}_${index + 2}${fileExtension}`; // Use the same extension

      // Send to Server 2
      await axios.post(SERVER_2_URL, {
        fileName: chunkFileName,
        chunkData: chunk.toString('base64'), // Convert chunk data to base64
      }).then(()=>{
        // Insert record into the database
        try {
            connection.execute('INSERT INTO chunkMetaData (file, chunk, server) VALUES (?, ?, ?)', [fileNameWithoutExtension, index+2, '2']);
            console.log(`Record for local chunk ${index} inserted into the database`);
        } catch (error) {
            console.error(`Error inserting record for local chunk ${index} into the database:`, error);
        }
      });
      console.log(`Chunk ${index + 2} sent to Server 2 successfully`);

      // Send to Server 4
      await axios.post(SERVER_4_URL, {
        fileName: chunkFileName,
        chunkData: chunk.toString('base64'), // Convert chunk data to base64
      }).then(()=>{
        // Insert record into the database
        try {
            connection.execute('INSERT INTO chunkMetaData (file, chunk, server) VALUES (?, ?, ?)', [fileNameWithoutExtension, index+2, '4']);
            console.log(`Record for local chunk ${index} inserted into the database`);
        } catch (error) {
            console.error(`Error inserting record for local chunk ${index} into the database:`, error);
        }
      });;
      console.log(`Chunk ${index + 2} sent to Server 4 successfully`);
    }));
  } catch (error) {
    console.error('Error sending chunks to servers:', error.message);
    return res.status(500).json({ error: 'Error sending chunks to servers' });
  }

  return res.status(200).json({ message: 'File uploaded and divided into chunks' });
});


// Set up a route for file download
app.get('/download/:fileName', async (req, res) => {
    const fileName = req.params.fileName;
    let dbRecords;
   
    query = `SELECT * FROM chunkMetaData WHERE file = '${fileName}'`;
    dbRecords = connection.query(query, (error, response) => {
        console.log('records from database', error || response);
        var table = JSON.parse(JSON.stringify(response));
        return table ;
    });
  
    try {
        const files = await fs.promises.readdir(path.join(__dirname, 'uploads'));
        const chunkFiles = files.filter((file) => file.startsWith(fileName));
      
        // Check if any chunk files were found
        if (chunkFiles.length === 0) {
          throw new Error('Chunk files not found');
        }
      
        // Read and combine all chunk files
        const combinedData = Buffer.concat(chunkFiles.map((chunkFile) =>
          fs.readFileSync(path.join(__dirname, 'uploads', chunkFile))
        ));
        // Get the missing chunks' data from SERVER_2_URL
        const chunk2URL = `http://localhost:5000/chunks/${fileName}`;
        const chunk2Response = await axios.get(chunk2URL);

        // Return only the data from the Axios response
        const chunk2Data = chunk2Response.data.chunkData;
        const actualContent = Buffer.from(chunk2Data, 'base64');

        // Combine combinedData and actualContent
        const combinedContent = Buffer.concat([combinedData, actualContent]);

        // Get the file extension from one of the chunk files
        const fileExtension = path.extname(chunkFiles[0]); 

        // Set response headers for download
        res.setHeader('Content-Type', 'application/octet-stream');
        res.setHeader('Content-Disposition', `attachment; filename="${fileName}${fileExtension}"`);
        // Send the combined content as the response
        res.send(combinedContent);
    } catch (error) {
      console.error('Error downloading file:', error.message);
      return res.status(500).json({ error: 'Error downloading file' });
    }
});
  
  
app.get('/chunks/:fileName', (req, res) => {
    const fileName = req.params.fileName;
    const combinedFilePath = path.join(UPLOADS_DIR, fileName);
  
    // Read all files in the uploads directory
    fs.readdir(UPLOADS_DIR, (err, files) => {
      if (err) {
        console.error('Error reading directory:', err);
        return res.status(500).json({ error: 'Error reading directory' });
      }
  
      // Filter chunk files with the same base name
      const chunkFiles = files.filter((file) => file.startsWith(fileName));
  
      // Check if any chunk files were found
      if (chunkFiles.length === 0) {
        return res.status(404).json({ error: 'Chunk files not found' });
      }
  
      // Read and combine all chunk files
      const chunksData = chunkFiles.map((chunkFile) => fs.readFileSync(path.join(UPLOADS_DIR, chunkFile)));
  
      // Combine all chunks into a single buffer
      const combinedData = Buffer.concat(chunksData);
  
      // Convert the combined data to base64
      const base64Data = combinedData.toString('base64');
  
      // Return the base64 data
      res.json({ chunkData:base64Data });
    });
  });
  




app.listen(PORT, () => {
  console.log(`Master Server is running on http://localhost:${PORT}`);
});
