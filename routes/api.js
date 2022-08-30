import express from "express";

let router = express.Router();
{
  router.get('/:tst', function (req, res, next) {
    res.json({'retval': `got here with ${req.params.tst}!`});
  });

  router.get('/', function (req, res, next) {
    res.json({'retval': `You sent no parameters...`});
  });
}

export default router;
