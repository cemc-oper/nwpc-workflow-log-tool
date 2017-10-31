import {TimeLine} from 'nuwe-timeline';
import {JSDOM, VirtualConsole} from 'jsdom'
const d3_selection = require('d3-selection');

const fs = require('fs');
const gm = require('gm').subClass({imageMagick: true});;

let class_styles = [
  {class_name:'unknown', color: '#bdbdbd'},
  {class_name:'serial',  color: '#4575b4'},
  {class_name:'serial_op',  color: '#74add1'},
  {class_name:'serial_op1', color: '#deebf7'},
  {class_name:'operation', color: '#cc4c02'},
  {class_name:'operation1',  color: '#f46d43'},
  {class_name:'operation2',  color: '#feb24c'},
  {class_name:'normal',  color: '#cb181d'},
  {class_name:'largemem', color: '#67000d'}
];

const virtualConsole = new VirtualConsole();
virtualConsole.sendTo(console);

const dom = new JSDOM(``,{ virtualConsole });

const document = dom.window.document;
global.document = document;

let body = d3_selection.select(document.body);
let container = body.append('div')
  .attr('id', 'container');

let data = require("./data.json");
let my_timeline = new TimeLine('#container',{
  type: 'timeline',
  data: {
    class_styles: class_styles,
    data: data
  }
});

fs.writeFileSync('./dist/output.svg', container.html());

gm('./dist/output.svg').write('./dist/output.png', function(err){
  if (!err) console.log('image converted.')
  else console.log(err);
})