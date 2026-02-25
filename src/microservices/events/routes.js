const express = require('express');

function createRoutes(producer) {
  const router = express.Router();

  router.get('/health', (req, res) => {
    res.json({ status: true });
  });

  router.post('/movie', async (req, res) => {
    try {
      const movieEvent = req.body;

      if (!movieEvent.movie_id || !movieEvent.title || !movieEvent.action) {
        return res.status(400).json({
          error: 'Missing required fields: movie_id, title, and action are required',
        });
      }

      const event = {
        id: `movie-${movieEvent.movie_id}-${Date.now()}`,
        type: 'movie',
        timestamp: new Date().toISOString(),
        payload: movieEvent,
      };

      const result = await producer.sendEvent('movie-events', event);

      res.status(201).json({
        status: 'success',
        partition: result.partition,
        offset: parseInt(result.offset),
        event: event,
      });
    } catch (error) {
      console.error('Error creating movie event:', error);
      res.status(500).json({ error: 'Internal Server Error' });
    }
  });

  router.post('/user', async (req, res) => {
    try {
      const userEvent = req.body;

      if (!userEvent.user_id || !userEvent.action || !userEvent.timestamp) {
        return res.status(400).json({
          error: 'Missing required fields: user_id, action, and timestamp are required',
        });
      }

      const event = {
        id: `user-${userEvent.user_id}-${Date.now()}`,
        type: 'user',
        timestamp: userEvent.timestamp,
        payload: userEvent,
      };

      const result = await producer.sendEvent('user-events', event);

      res.status(201).json({
        status: 'success',
        partition: result.partition,
        offset: parseInt(result.offset),
        event: event,
      });
    } catch (error) {
      console.error('Error creating user event:', error);
      res.status(500).json({ error: 'Internal Server Error' });
    }
  });

  router.post('/payment', async (req, res) => {
    try {
      const paymentEvent = req.body;

      if (
        !paymentEvent.payment_id ||
        !paymentEvent.user_id ||
        !paymentEvent.amount ||
        !paymentEvent.status ||
        !paymentEvent.timestamp
      ) {
        return res.status(400).json({
          error:
            'Missing required fields: payment_id, user_id, amount, status, and timestamp are required',
        });
      }

      const event = {
        id: `payment-${paymentEvent.payment_id}-${Date.now()}`,
        type: 'payment',
        timestamp: paymentEvent.timestamp,
        payload: paymentEvent,
      };

      const result = await producer.sendEvent('payment-events', event);

      res.status(201).json({
        status: 'success',
        partition: result.partition,
        offset: parseInt(result.offset),
        event: event,
      });
    } catch (error) {
      console.error('Error creating payment event:', error);
      res.status(500).json({ error: 'Internal Server Error' });
    }
  });

  return router;
}

module.exports = createRoutes;
