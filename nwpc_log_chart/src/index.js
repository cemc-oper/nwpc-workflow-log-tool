const nuwe_timeline = require('nuwe-timeline');
const {TimeLine} = nuwe_timeline;

const jsdom = require('jsdom');
const { JSDOM } = jsdom;
const fs = require('fs');
const d3_selection = require('d3-selection');


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

const virtualConsole = new jsdom.VirtualConsole();
virtualConsole.sendTo(console);

const dom = new JSDOM(``,{ virtualConsole });


//
const document = dom.window.document;
let body = d3_selection.select(document.body);
let container = body.append('div')
  .attr('id', 'container');
console.log(container);

// const d3_shape = require('d3-shape');
//
// var pieData = [12,31];
// var outputLocation = 'test.svg';
//
// var chartWidth = 500, chartHeight = 500;
//
// var arc = d3_shape.arc()
//   .outerRadius(chartWidth/2 - 10)
//   .innerRadius(0);
//
// var colours = ['#F00','#000','#000','#000','#000','#000','#000','#000','#000'];
//
// var s = container.append('svg');
// s.attr('id', '#container_svg')
//   .attr({
//     width:chartWidth,
//     height:chartHeight
//   });
// console.log(s);
// s.append('g')
//   .attr('transform','translate(' + chartWidth/2 + ',' + chartWidth/2 + ')');
//
// s.selectAll('.arc')
//   .data( d3_shape.pie()(pieData) )
//   .enter()
//   .append('path')
//   .attr({
//     'class':'arc',
//     'd':arc,
//     'fill':function(d,i){
//       return colours[i];
//     },
//     'stroke':'#fff'
//   });
//
// //write out the children of the container div
// fs.writeFileSync(outputLocation, body.select('#container').html()) //using sync to keep the code simple


let data = require("./data.json");
global.document = document;
let my_timeline = new TimeLine('#container',{
  type: 'timeline',
  data: {
    class_styles: class_styles,
    data: data
  }
});

fs.writeFileSync('./output.svg', container.html());
