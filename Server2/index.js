const express = require('express');
const fs = require('fs');
const path = require('path');
const axios = require('axios');

const app = express();
const PORT = 5000;

const UPLOADS_DIR = path.join(__dirname, 'uploads');

// Middleware to parse JSON bodies
app.use(express.json());

// Set up a route to handle receiving and saving chunks
app.post('/upload', (req, res) => {
  const { fileName, chunkData } = req.body;

  if (!fileName || !chunkData) {
    return res.status(400).json({ error: 'Missing fileName or chunkData' });
  }

  // Convert base64 chunk data back to buffer
  const chunkBuffer = Buffer.from(chunkData, 'base64');

  // Ensure the uploads directory exists
  if (!fs.existsSync(UPLOADS_DIR)) {
    fs.mkdirSync(UPLOADS_DIR, { recursive: true });
  }

  // Construct the file path and write the chunk to file
  const chunkFilePath = path.join(UPLOADS_DIR, fileName);
  fs.writeFile(chunkFilePath, chunkBuffer, { flag: 'a' }, (err) => {
    if (err) {
      console.error('Error saving chunk:', err);
      return res.status(500).json({ error: 'Error saving chunk' });
    }
    console.log(`Chunk ${fileName} saved successfully`);
    res.status(200).json({ message: 'Chunk saved successfully' });
  });
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


// Set up a route for file download
app.get('/download/:fileName', async (req, res) => {
  const fileName = req.params.fileName;

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
      const chunk2URL = `http://localhost:4000/chunks/${fileName}`;
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


app.listen(PORT, () => {
  console.log(`Server 2 is running on http://localhost:${PORT}`);
});
