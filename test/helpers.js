'use strict';
const fs = require('fs'); const path = require('path');
const ROOT = path.join(__dirname, '..');
const P = require('../js/parser.js'); const E = require('../js/eventlog.js'); const D = require('../js/diagnose.js');
const read = (rel) => fs.readFileSync(path.join(ROOT, rel), 'utf8');
const plan = (rel) => P.parsePlan(read(rel));
const diag = (rel) => D.diagnose(plan(rel));
const rules = (d) => d.findings.map((f) => f.rule);
const find = (d, rule) => d.findings.filter((f) => f.rule === rule);
const sampleFiles = () => fs.readdirSync(path.join(ROOT, 'samples')).filter((f) => f.endsWith('.txt')).map((f) => 'samples/' + f)
  .concat(fs.readdirSync(path.join(ROOT, 'samples/fixes')).map((f) => 'samples/fixes/' + f));
module.exports = { ROOT, P, E, D, read, plan, diag, rules, find, sampleFiles };
