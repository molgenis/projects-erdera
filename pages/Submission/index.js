// set the name of script
const scriptName = "template builder";
const targetSchema = "rd3";


// set template mappings: the job requires schema, tables
const templates = {
    lrGS: "Samples lrGS,Experiments lrGS",
    OGM: "Samples OGM,Experiments OGM",
    RNA: "Samples RNA,Experiments RNA",
    srDNA: "Samples srDNA,Experiments srDNA",
}


// get elements and set default behaviours
let selectedTemplate;
const formElem = document.getElementById("template-form");
const submitBtn = document.getElementById("download");
const selectInput = document.getElementById("templateSelect");
const fileDownloadElem = document.getElementById("fileDownload");
const fileDownloadLink = document.getElementById("downloadLink");
const busyElem = document.getElementById("busyIndicator");
const errorElem = document.getElementById("errorMessage");
const errorTextElem = errorElem.querySelector("span");

formElem.addEventListener("submit", (event) => event.preventDefault());
selectInput.addEventListener("input", (event) => {
    selectedTemplate = event.target.value;
    if (selectedTemplate !== "") {
        submitBtn.removeAttribute("disabled");
    } else {
        if (!submitBtn.getAttribute("disabled")) {
            submitBtn.setAttribute("disabled", true);
        }
    }
});

// start script by name
async function runScript(name, schema, tables) {
    const url = `/api/scripts/${encodeURI(name)}`;
    const response = await fetch(url, { method: "POST", body: `${schema};${tables}` });
    return response.json();
}

// check the current status of a task by url: /api/tasks/<task-id>
async function getTaskStatus(taskId) {
    const response = await fetch(`/api/tasks/${taskId}`);
    return response.json();
}

// retrieve task output
async function getTaskOutput(taskId) {
    const query = "query ($filter: JobsFilter) { Jobs(filter: $filter) { id output { id size filename extension url } } }";
    const variables = {"filter": {"id": { "equals": `${taskId}`}}}

    const response = await fetch(`/_SYSTEM_/api/graphql`, {
        method: "POST",
        body: JSON.stringify({ query: query, variables: variables })
    });
    return response.json();
}


// recheck task status: continue to check task status for n times
async function recheckTaskStatus(id, delay, retries) {
    const maxRetries = retries;
    let count = 0;
    let taskStatus = {}
    while (count < maxRetries) {
        console.log(`Checking task status ${id} (${count})`)
        try {
            const taskJson = await getTaskStatus(id);
            if (["COMPLETED", "SKIPPED", "WARNING", "ERROR", "UNKNOWN"].includes(taskJson.status)) {
                console.log('Task has finished', taskJson.status);
                count = maxRetries
                taskStatus = taskJson;
            }
            await new Promise(resolve => setTimeout(resolve, delay));
            count += 1;
        } catch (err) {
            throw new Error(err);
        }
    }
    return taskStatus;
}

submitBtn.addEventListener("click", async (event) => {
    event.preventDefault();
    
    // reset styles
    busyElem.classList.remove("d-none");
    busyElem.classList.add("d-flex");
    errorElem.classList.add("d-none");
    errorTextElem.innerText = "";
    fileDownloadElem.classList.add("d-none");

    try {
        console.log("Running script", scriptName);
        const result = await runScript(scriptName, schema=targetSchema, tables=templates[selectedTemplate]);
    
        if (result) {
            console.log("Waiting for task to complete....");
            const taskStatus = await recheckTaskStatus(result.id, 3750, 10);
    
            if (taskStatus) {
                console.log('Retrieving output for task', result.id);
                const taskOutput = await getTaskOutput(taskStatus.id);
                if (Object.hasOwn(taskOutput, "data")) {
                    if (Object.hasOwn(taskOutput.data, "Jobs")) {
                        const jobOutput = taskOutput.data.Jobs[0].output;
                        const filename = `${targetSchema} ${selectedTemplate}.${jobOutput.extension}`;
                        fileDownloadLink.setAttribute("href", jobOutput.url);
                        fileDownloadLink.setAttribute("download", filename);
                        fileDownloadLink.innerText = filename;
                        fileDownloadElem.classList.remove("d-none");
                    }
                }
            }
        }
    } catch(error) {
        errorTextElem.innerText = error;
        errorElem.classList.remove("d-none");
        throw new Error(error);
    } finally {
        busyElem.classList.remove("d-flex");
        busyElem.classList.add("d-none");
    }
});