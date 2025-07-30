const express = require('express');
const bodyParser = require('body-parser');
const { Client } = require('@elastic/elasticsearch');

const app = express();
const port = 3000;

app.use(bodyParser.json());

// Configure Elasticsearch client
const esClient = new Client({
  node: 'http://localhost:9200', // Adjust if your ES runs elsewhere
});

// Test connection on startup
(async () => {
  try {
    const health = await esClient.cluster.health();
    console.log('Elasticsearch cluster health:', health.status);
  } catch (err) {
    console.error('Elasticsearch connection error:', err);
  }
})();

// Sample index and mapping for nested documents
const INDEX_NAME = 'sample_nested_docs';

// Create index with nested mapping if it doesn't exist
async function ensureIndex() {
  const exists = await esClient.indices.exists({ index: INDEX_NAME });
  if (!exists.body) {
    await esClient.indices.create({
      index: INDEX_NAME,
      body: {
        mappings: {
          properties: {
            name: { type: 'text' },
            age: { type: 'integer' },
            children: {
              type: 'nested',
              properties: {
                name: { type: 'text' },
                grade: { type: 'integer' },
                hobbies: { type: 'text' }
              }
            }
          }
        }
      }
    });
  }
}

// Endpoint to insert sample nested documents
app.post('/insert-sample', async (req, res) => {
  try {
    await ensureIndex();
    const sampleDoc = {
      name: 'John Doe',
      age: 40,
      children: [
        { name: 'Alice', grade: 5, hobbies: 'drawing' },
        { name: 'Bob', grade: 3, hobbies: 'soccer' }
      ]
    };
    const response = await esClient.index({
      index: INDEX_NAME,
      body: sampleDoc
    });
    await esClient.indices.refresh({ index: INDEX_NAME });
    res.json({ success: true, response });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: err.message });
  }
});



// Endpoint to query nested documents with filters/search on parent and child
app.post('/search', async (req, res) => {
  const { parent, child } = req.body; // parent: {name, age}, child: {name, grade, hobbies}
  try {
    const must = [];
    if (parent) {
      if (parent.name) must.push({ match: { name: parent.name } });
      if (parent.age) must.push({ term: { age: parent.age } });
    }
    if (child) {
      const childMust = [];
      if (child.name) childMust.push({ match: { 'children.name': child.name } });
      if (child.grade) childMust.push({ term: { 'children.grade': child.grade } });
      if (child.hobbies) childMust.push({ match: { 'children.hobbies': child.hobbies } });
      if (childMust.length > 0) {
        must.push({
          nested: {
            path: 'children',
            query: {
              bool: { must: childMust }
            }
          }
        });
      }
    }
    console.log(JSON.stringify(must, null, 2));
    
    const query = must.length > 0 ? { bool: { must } } : { match_all: {} };
    const result = await esClient.search({
      index: INDEX_NAME,
      body: { query }
    });
    console.log(result);
    
    res.json(result.hits.hits.map(hit => hit._source));
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/', (req, res) => {
  res.send('Express server for Elasticsearch is running.');
});

app.listen(port, () => {
  console.log(`Server listening at http://localhost:${port}`);
}); 