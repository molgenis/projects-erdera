import jsdoc2md from "jsdoc-to-markdown";
import { promises as fs, existsSync, mkdirSync } from "node:fs";
import path from "path";

const entryDir = "./src/js/";
const outputDir = "./docs/expressions/";

try {
  const inputDirPath = path.resolve(`${entryDir}/*.js`);
  const outputFilePath = path.resolve(`${outputDir}/README.md`);
  console.log("Reading files from ", inputDirPath);

  if (!existsSync(outputDir)) {
    console.log("Creating output directory");
    mkdirSync(outputDir);

    if (!existsSync(outputFilePath)) {
      console.log("creating output file");
      await fs.writeFile(outputFilePath, "");
    }
  }

  jsdoc2md
    .render({ files: inputDirPath })
    .then((output) => {
      const content = output.split("\n");
      const start = content.findIndex((value) => value === "<dl>");
      const end = content.findIndex((value) => value === "</dl>");

      const doc = content.map((line, index) => {
        if (index < start || index > end) {
          if (line === "## Members") {
            return "# ERDERA-RD3 Expressions";
          } else {
            const isNotDocTag = line.search(/^\*{2}(Kind|Tag)\*{2}/) === -1;
            const isNotAnchorElem = line.search(/^(<a (.*)><\/a>)$/) === -1;

            if (isNotDocTag && isNotAnchorElem) {
              let newLine = line.replace(/<code>/, "`");
              newLine = newLine.replace(/<\/code>/, "`");
              return newLine;
            }
          }
        }
      });
      return doc.join("\n");
    })
    .then((result) => {
      console.log("Writing file to", outputFilePath);
      fs.writeFile(outputFilePath, result);
    });
} catch (err) {
  console.error(err);
}
