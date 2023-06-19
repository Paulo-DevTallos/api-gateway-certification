import express from 'express';
import { CompressionTypes } from 'kafkajs';

const routes = express.Router();

routes.post('/certifications', async (req, res) => {
  const message = {
    user: { id: 1, name: 'Paulo Sergio' },
    course: 'Kafka com Node.js',
    grade: 5,
  };

  // Chamar microsserviço
  await req.producer.send({
    topic: 'issue-certificate',
    compression: CompressionTypes.GZIP,
    messages: [
      { value: JSON.stringify(message) },
      { value: JSON.stringify({ ...message, user: { ...message.user, name: 'Rafa Mendes' } }) },
    ],
  })

  return res.json({ ok: true });
});

export default routes;