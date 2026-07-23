let totals = {
    Individuals: "-",
    Samples: "-",
    Experiments: "-",
    Files: "-"
};

async function getData () {
    const query = `{
        Individuals_agg {
            count
        }
        Samples_agg {
            count
        }
        Experiments_agg {
            count
        }
        Files_agg {
            count
        }
    }`;

    const response = await fetch("/erdera/api/graphql",
        { 
            method: "POST",
            body: JSON.stringify({ query: query })
        }
    );
    const result = await response.json();

    if (result && result.data) {
        const data = Object.keys(result.data).map((key) => {
            return [key.split("_agg")[0], result.data[key].count]
        });
        totals = Object.fromEntries(data);
    }
}

setTimeout(() => {
    getData()
    .then(() => {
        if (totals.Individuals > 0) {
            const databaseCountsElem = document.getElementById("databaseCounts");
            databaseCountsElem.classList.remove("d-none");

        }

        const individualsElem = document.getElementById("individuals");
        const samplesElem = document.getElementById("samples");
        const experimentsElem = document.getElementById("experiments");
        const filesElem = document.getElementById("files");

        individualsElem.setAttribute("value", totals.Individuals.toLocaleString());
        samplesElem.setAttribute("value", totals.Samples.toLocaleString());
        experimentsElem.setAttribute("value", totals.Experiments.toLocaleString());
        filesElem.setAttribute("value", totals.Files.toLocaleString());
    })
    .catch((err) => {
        throw new Error(err);
    });
}, 150)