const express = require('express');
const bodyParser = require('body-parser');
const { Client } = require('@elastic/elasticsearch');

const app = express();
const port = 3000;

app.use(bodyParser.json());

// Configure Elasticsearch client
const esClient = new Client({
  node: 'http://localhost:9200', // Change if your ES is elsewhere
});

// Detect ES version helper
async function isES7() {
  const info = await esClient.info();
  return info.version.number.startsWith('7.');
}

// Check if index exists (works for ES7 & ES8)
async function indexExists(index) {
  const exists = await esClient.indices.exists({ index });
  return typeof exists.body !== 'undefined' ? exists.body : exists;
}

// Ensure both indexes exist
async function ensureIndexes() {
  const es7 = await isES7();

  // Company index
  const companyExists = await indexExists('company');
  if (!companyExists) {
    await esClient.indices.create({
      index: 'company',
      body: {
        mappings: {
          properties: {
            id: { type: 'keyword' },
            name: { type: 'text', fields: { keyword: { type: 'keyword' } } }
          }
        }
      }
    });
    console.log('Created company index');
  }

  // Reports index
  const reportsExists = await indexExists('reports');
  if (!reportsExists) {
    await esClient.indices.create({
      index: 'reports',
      body: {
        mappings: {
          properties: {
            id: { type: 'keyword' },
            name: { type: 'text', fields: { keyword: { type: 'keyword' } } },
            company_id: { type: 'keyword' },
            tags: { 
              type: 'text',
              fields: { keyword: { type: 'keyword' } }
            },
            status: { type: 'keyword' }
          }
        }
      }
    });
    console.log('Created reports index');
  }
}

// Insert sample data
app.post('/insertSampleData', async (req, res) => {
  try {
    // Create 5 sample companies
    const companies = [];
    for (let i = 1; i <= 5; i++) {
      companies.push({
        id: `C${i}`,
        name: `Company ${i}`
      });
    }

    // Bulk insert companies
    const companyBulkOps = companies.flatMap(company => [
      { index: { _index: 'company', _id: company.id } },
      company
    ]);
    const companyResp = await esClient.bulk({ refresh: true, body: companyBulkOps });
    if (companyResp.errors) {
      console.error('Some company inserts failed:', companyResp.items);
    }

    // Create 3 reports for each company
    const reports = [];
    let reportCounter = 1;
    companies.forEach(company => {
      for (let r = 1; r <= 3; r++) {
        reports.push({
          id: `R${reportCounter}`,
          name: `Report ${reportCounter}`,
          company_id: company.id,
          tags: [`tag${r}`, 'common'], // Array of words
          status: r % 2 === 0 ? 'draft' : 'published'
        });
        reportCounter++;
      }
    });

    // Bulk insert reports
    const reportsBulkOps = reports.flatMap(report => [
      { index: { _index: 'reports', _id: report.id } },
      report
    ]);
    const reportsResp = await esClient.bulk({ refresh: true, body: reportsBulkOps });
    if (reportsResp.errors) {
      console.error('Some report inserts failed:', reportsResp.items);
    }

    res.json({ success: true, companies, reports });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/search', async (req, res) => {
  try {
    const {
      companyFilters = {},
      reportFilters = {},
      page = 1,
      size = 10,
      sortField = 'name.keyword',
      sortOrder = 'asc'
    } = req.body;

    // Step 1: Search reports first
    const reportMust = [];
    if (reportFilters.id) {
      reportMust.push({ term: { id: reportFilters.id } });
    }
    if (reportFilters.name) {
      reportMust.push({ match: { name: reportFilters.name } });
    }
    if (reportFilters.tags && reportFilters.tags.length) {
      reportMust.push({ terms: { 'tags.keyword': reportFilters.tags } });
    }
    if (reportFilters.status) {
      reportMust.push({ term: { status: reportFilters.status } });
    }

    const reportQuery = {
      index: 'reports',
      _source: ['id', 'name', 'company_id', 'tags', 'status'],
      size: 10000, // to get all matches; can optimize
      query: reportMust.length ? { bool: { must: reportMust } } : { match_all: {} }
    };

    const reportResp = await esClient.search(reportQuery);

    const matchingReports = reportResp.hits.hits.map(hit => hit._source);
    const matchingCompanyIds = [...new Set(matchingReports.map(r => r.company_id))];

    if (!matchingCompanyIds.length) {
      return res.json({ success: true, total: 0, companies: [] });
    }

    // Step 2: Search companies that match both company filters and report results
    const companyMust = [];
    if (companyFilters.id) {
      companyMust.push({ term: { id: companyFilters.id } });
    }
    if (companyFilters.name) {
      companyMust.push({ match: { name: companyFilters.name } });
    }

    // Only include companies that have matching reports
    companyMust.push({ terms: { id: matchingCompanyIds } });

    const companyQuery = {
      index: 'company',
      from: (page - 1) * size,
      size,
      sort: [{ [sortField]: { order: sortOrder } }],
      query: { bool: { must: companyMust } }
    };

    const companyResp = await esClient.search(companyQuery);
    const companies = companyResp.hits.hits.map(hit => hit._source);

    // Step 3: Attach matching reports to companies
    const companiesWithReports = companies.map(company => ({
      ...company,
      reports: matchingReports.filter(r => r.company_id === company.id)
    }));

    res.json({
      success: true,
      total: companyResp.hits.total.value,
      companies: companiesWithReports
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Root endpoint
app.get('/', (req, res) => {
  res.send('Express server for Elasticsearch is running.');
});

// Start server
(async () => {
  try {
    const health = await esClient.cluster.health();
    console.log('Elasticsearch cluster health:', health.status || health.body?.status);

    await ensureIndexes();
    console.log('Indexes ensured.');

    app.listen(port, () => {
      console.log(`Server listening at http://localhost:${port}`);
    });
  } catch (err) {
    console.error('Startup error:', err);
  }
})();
