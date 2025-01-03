const express = require('express');
const morgan = require('morgan');
const cors = require('cors');
const leadsRouter = require('./routes/leadRoutes');

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
    res.status(200).send('Hello from Rate Cut');
});

app.use('/api/v1/leads', leadsRouter);

module.exports = app;