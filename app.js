const express = require('express');
const morgan = require('morgan');
const cors = require('cors');
const leadsRouter = require('./routes/leadRoutes');
const apiKeyRoutes = require('./routes/apiKeyRoutes');
const formRouter = require('./routes/formRoutes');
const distributionRuleRouter = require('./routes/distributionRuleRoutes');
const rcsRouter = require('./routes/rcsRoutes');
const rcsScheduler = require('./scheduler/rcsScheduler');
// const ivrRoutes = require('./routes/ivrRoutes');
const exportRoutes = require('./routes/exportRoutes');
const distributionRoutes = require('./routes/distributionRoutes');
const lenderRequestRoutes = require('./routes/lenderRequestRoutes');

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

rcsScheduler.init();

module.exports = app;