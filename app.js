const express = require('express');
const morgan = require('morgan');
const cors = require('cors');
const leadsRouter = require('./routes/leadRoutes');
const apiKeyRoutes = require('./routes/apiKeyRoutes');
const formRouter = require('./routes/formRoutes');
const distributionRuleRouter = require('./routes/distributionRuleRoutes');
const rcsRouter = require('./routes/rcsRoutes');
const rcsScheduler = require('./scheduler/rcsScheduler');
// const continuousScheduler = require('./scheduler/continuousScheduler');
// const ivrRoutes = require('./routes/ivrRoutes');
const exportRoutes = require('./routes/exportRoutes');
const distributionRoutes = require('./routes/distributionRoutes');
const lenderRequestRoutes = require('./routes/lenderRequestRoutes');
const statsRoutes = require('./routes/statsRoutes');
// const pendingLeadRoutes = require('./routes/pendingLeadsRoutes');
const leadSuccessRoutes = require('./routes/leadSuccessRoutes');
const lenderStatusUploadRoutes = require('./routes/lenderStatusUploadRoutes');
const leadPortalRoutes = require('./routes/leadPortalRoutes');

const app = express();
if (process.env.NODE_ENV === 'development') {
    app.use(morgan('dev'));
}
app.use(express.json());

app.use(cors({
    origin: '*'
}));

app.use((req, res, next) => {
    console.log('Hello from the middleware 👋');
    next();
});

// Root route to respond with "Hello from Rate Cut"
app.get('/', (req, res) => {
    res.status(200).send('Hello from Rate Cut!');
});

app.use('/api/v1/leads', leadsRouter);
app.use('/api/v1/api-keys', apiKeyRoutes);
app.use('/api/v1/loan-form', formRouter);
app.use('/api/v1/distribution-rules', distributionRuleRouter);
app.use('/api/v1/rcs', rcsRouter);
// app.use('/api/v1/ivr', ivrRoutes);
app.use('/api/v1/export', exportRoutes);
app.use('/api/v1/distribution', distributionRoutes);
app.use('/api/v1/lenderRequest', lenderRequestRoutes);
app.use('/api/v1/unified-stats', statsRoutes);
// app.use('/api/v1/pending-leads', pendingLeadRoutes);
app.use('/api/v1/leads_success', leadSuccessRoutes);
app.use('/api/v1/uploadStatus', lenderStatusUploadRoutes);
app.use('/api/v1/lendportal', leadPortalRoutes);

rcsScheduler.init();
// // Start continuous scheduler when server starts
// continuousScheduler.start();

// // Graceful shutdown
// process.on('SIGTERM', () => {
//   continuousScheduler.stop();
//   server.close(() => {
//     process.exit(0);
//   });
// });

module.exports = app;