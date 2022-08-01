function makeLogger() {
  const preFix = `VU ${__VU}: `;
  function log(str) {
    console.log(`${preFix}` + str);
  }
  return log;
}
export default makeLogger();
