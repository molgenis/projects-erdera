import jsdoc2md from 'jsdoc-to-markdown'
import { promises as fs, existsSync, mkdirSync } from 'node:fs';
import path from 'path'

const entryDir = "./model/expressions/*.js"
const outputDir = "./docs"

try {
  const outputFilePath = path.resolve(`${outputDir}/expressions.md`);
  
  if (!existsSync(outputDir)) {
    mkdirSync(outputDir);
      
    if (!existsSync(outputFilePath)) {
      await fs.writeFile(outputFilePath, '')
    }
  }
  
  jsdoc2md.render({ files: entryDir })
  .then((output) => {
    const content = output.split("\n");
    const docs = [];
    
    for (let i=0; i < content.length; i++) {
      const line = content[i];
      const isNotHtml = line.search(/^(<(.*?)>(.|\n)*?<\/(.*?)>)$/) === -1 && line.search(/^(<(.*)>)$/) === -1;
      const isNotDocTag = line.search(/^\*\*/) === -1;
      if (line === "## Members") {
       docs.push("# Expressions documentation\n");
      // } else {
      //   docs.push(line)
      // }
      } else if (isNotHtml && isNotDocTag && line !== "") {
        let newLine = line.replace(/<code>/, '`');
        newLine = newLine.replace(/<\/code>/, '`');
        docs.push(newLine)
      } else {
        docs.push(line)
      }
    } 
    // console.log(docs);
    return docs.join("\n")
  })
  .then((result) => {
    fs.writeFile(outputFilePath, result)
  });
  
} catch (err) {
  console.error(err);
} 