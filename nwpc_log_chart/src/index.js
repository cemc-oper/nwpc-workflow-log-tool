import {TimeLine} from 'nuwe-timeline';
import {JSDOM, VirtualConsole} from 'jsdom'
const d3_selection = require('d3-selection');

const fs = require('fs');
const gm = require('gm')//.subClass({imageMagick: true});
// const path = require('path');

const ArgumentParser = require('argparse').ArgumentParser;

let parser = new ArgumentParser({
  addHelp:true,
  description: 'NWPC operation system run time line chart generator.'
});

parser.addArgument(
  ['-d', '--data'],
  {
    help: 'chart data'
  }
)

parser.addArgument(
  ['--output-svg'],
  {
    help: 'output svg file path'
  }
)

parser.addArgument(
  ['--output-png'],
  {
    help: 'output png file path'
  }
)

let args = parser.parseArgs();

let data_file_path = args.data;
let output_svg_file_path = args.output_svg;
let output_png_file_path = args.output_png;

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

let data = JSON.parse(fs.readFileSync(data_file_path, 'utf8'))

let my_timeline = new TimeLine('#container',{
  type: 'timeline',
  data: {
    class_styles: class_styles,
    data: data
  }
});

let svg_content = container.html();

// let svg_dir = path.dirname(output_svg_file_path);
// if(!fs.existsSync(svg_dir)){
//   fs.mkdirSync(svg_dir);
// }

fs.writeFileSync(output_svg_file_path, svg_content);

// let png_dir = path.dirname(output_png_file_path);
// if(!fs.existsSync(png_dir)){
//   fs.mkdirSync(png_dir);
// }

console.log(output_svg_file_path, output_png_file_path);

gm(output_svg_file_path).write(output_png_file_path, function(err){
  if (!err)
    console.log('image converted.')
  else {
    console.log('gm has some error');
    console.log(err);
  }
})