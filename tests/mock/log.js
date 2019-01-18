class Logger {
    static GetLogFromContainer() {
        return {
            debug: (arg) => { },// eslint-disable-line,
            error: (arg) => { console.log(arg) },
            info: (arg) => { console.log(arg) }
        };

        // return { debug: arg => console.log(arg) }; // eslint-disable-line
    }
}
module.exports = Logger;
