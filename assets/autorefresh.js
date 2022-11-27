console.log("hello")

var rowDataBidsX=[];
var rowDataAsksX=[];

const gridBids= new gridjs.Grid({
                  columns: ["Bids", "Price"],
                  data: rowDataBidsX
              }).render(document.getElementById("myOrderbookBids"));
const gridAsks = new gridjs.Grid({
                  columns: ["Asks", "Price"],
                  data: rowDataAsksX
              }).render(document.getElementById("myOrderbookAsks"));


setInterval(getOrderBookData, 1000);
async function getOrderBookData() {
  const rowDataBids= [];
  const rowDataAsks= [];
  const response = await fetch("http://localhost:8080/orderbook/btcusd");
  const data = await response.json();
  const asks = data["asks"]["prices"];
  const bids = data["bids"]["prices"];
  for (var key in asks) {
    var quantity = asks[key]["volume"];
    var row = [quantity,key];
    rowDataAsks.push(row);
  }
  for (var key in bids) {
    var quantity = bids[key]["volume"];
    var row = [quantity,key];
    rowDataBids.push(row);
  }
  rowDataBidsX= rowDataBids;
  rowDataAsksX = rowDataAsks;
  console.log(rowDataBidsX);
  gridBids.updateConfig({
    search: true,
    data: rowDataBids
  })
}









setInterval(myTimer, 1000);
function myTimer() {
  const date = new Date();
  document.getElementById("demo").innerHTML = date.toLocaleTimeString();
}










