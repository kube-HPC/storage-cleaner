class Logger {
    static GetLogFromContainer() {
        return {
            debug: (arg) => { }// eslint-disable-line
        };

        // return { debug: arg => console.log(arg) }; // eslint-disable-line
    }
}
module.exports = Logger;
