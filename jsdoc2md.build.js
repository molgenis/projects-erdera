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
    const start = content.findIndex((value) => value === "<dl>");
    const end = content.findIndex((value) => value === "</dl>");
    
    const doc = content.map((line,index) => {
      if (index < start || index > end) {
        const isNotDocTag = line.search(/^\*{2}(Kind|Tag)\*{2}/) === -1;
        const isNotAnchorElem = line.search(/^(<a (.*)><\/a>)$/) === -1;
        
        if (line === "## Members") {
          return "# RD3 expressions documentation"
        } else {
          if (isNotDocTag && isNotAnchorElem) {
            let newLine = line.replace(/<code>/, '`');
            newLine = newLine.replace(/<\/code>/, '`');
            return newLine
          }
        }
      }
    });
    
    return doc.join("\n")
  })
  .then((result) => {
    fs.writeFile(outputFilePath, result)
  });
  
} catch (err) {
  console.error(err);
} 