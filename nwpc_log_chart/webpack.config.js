'use strict';
let path = require('path');

let module_config= {
  rules: [
    {
      test: /\.js$/,
      use: [
        "babel-loader"
      ],
      exclude: /node_modules/,
      include: __dirname
    },
    {
      test: /\.css/,
      use: [
          'isomorphic-style-loader',
          {
            loader: 'css-loader',
            options: {
              importLoaders: 1
            }
          }
        ]
    }
  ]
};

module.exports = {
  entry: './src/index.js',
  output: {
    path: path.join(__dirname, 'dist'),
    filename: 'time_line_chart_tool.js',
  },
  module: module_config,
  target: 'node',
  devtool: 'source-map'
};