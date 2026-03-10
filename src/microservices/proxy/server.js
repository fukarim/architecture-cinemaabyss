const express = require('express');
const { createProxyMiddleware } = require('http-proxy-middleware');

const app = express();

const PORT = process.env.PORT || 8000;
const MONOLITH_URL = process.env.MONOLITH_URL || 'http://localhost:8080';
const MOVIES_SERVICE_URL = process.env.MOVIES_SERVICE_URL || 'http://localhost:8081';
const EVENTS_SERVICE_URL = process.env.EVENTS_SERVICE_URL || 'http://localhost:8082';
const GRADUAL_MIGRATION = process.env.GRADUAL_MIGRATION === 'true';
const MOVIES_MIGRATION_PERCENT = parseInt(process.env.MOVIES_MIGRATION_PERCENT || '0', 10);

app.get('/health', (req, res) => {
  res.status(200).json({ 
    status: 'healthy',
    service: 'proxy',
    timestamp: new Date().toISOString()
  });
});

function shouldRouteToMoviesMicroservice(req) {
  if (!GRADUAL_MIGRATION) {
    return false;
  }

  const hash = req.headers['x-user-id'] || req.ip || Math.random().toString();
  const hashValue = hash.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0);
  const percentage = hashValue % 100;
  
  return percentage < MOVIES_MIGRATION_PERCENT;
}

app.use('/api/movies', (req, res, next) => {
  const targetUrl = shouldRouteToMoviesMicroservice(req) ? MOVIES_SERVICE_URL : MONOLITH_URL;
  
  const proxy = createProxyMiddleware({
    target: targetUrl,
    changeOrigin: true,
    pathRewrite: (path) => {
      return path;
    },
    onError: (err, req, res) => {
      console.error(`Proxy error for ${req.url}:`, err.message);
      res.status(502).json({ 
        error: 'Bad Gateway',
        message: 'Failed to connect to backend service'
      });
    },
    onProxyReq: (proxyReq, req, res) => {
      proxyReq.setHeader('X-Forwarded-By', 'cinemaabyss-proxy');
    }
  });
  
  proxy(req, res, next);
});

app.use('/api/events', createProxyMiddleware({
  target: EVENTS_SERVICE_URL,
  changeOrigin: true,
  onError: (err, req, res) => {
    console.error(`Proxy error for ${req.url}:`, err.message);
    res.status(502).json({
      error: 'Bad Gateway',
      message: 'Failed to connect to events service'
    });
  },
  onProxyReq: (proxyReq, req, res) => {
    proxyReq.setHeader('X-Forwarded-By', 'cinemaabyss-proxy');
  }
}));

app.use('/', createProxyMiddleware({
  target: MONOLITH_URL,
  changeOrigin: true,
  onError: (err, req, res) => {
    console.error(`Proxy error for ${req.url}:`, err.message);
    res.status(502).json({ 
      error: 'Bad Gateway',
      message: 'Failed to connect to monolith service'
    });
  },
  onProxyReq: (proxyReq, req, res) => {
    proxyReq.setHeader('X-Forwarded-By', 'cinemaabyss-proxy');
  }
}));

// Start server
app.listen(PORT, () => {
  console.log(`Proxy server running on port ${PORT}`);
});
