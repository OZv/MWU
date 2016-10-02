var mwz=(function(){
function vyt(){
var h=document.getElementsByTagName("h2");
for(var i=0;i<h.length;i++){
var g=h[i].offsetHeight;
var n=h[i].nextSibling;
if(n.offsetHeight>g*10&&!/href=\"#/.test(n.innerHTML)){
	n.style.display="none";
	h[i].childNodes[1].className="hjn";
}
}}
if(typeof(mwz)=="undefined"){
if(window.addEventListener)
	window.addEventListener("load",vyt,false);
else window.attachEvent("onload",vyt);
}
return{
v:function(c,f){
c.removeAttribute("onclick");
with(c.style){
	cursor="default";outline="1px dotted gray";
}
var p="soundc11/";
if(/^span:/.test(f)){
	var l= f.replace(/^span:/,"");
	p="audio/prons/"+l[0]+"/"+l+".mp3";
}else{
	if(/^gg/.test(f))p+="gg";
	else if(/^bix/.test(f))p+="bix";
	else if(/^[0-9]/.test(f))p+="number";
	else p+=f[0];
	p+='/'+f+".wav";
}
var u="http://media.merriam-webster.com/"+p;
var b=function(){
	with(c.style){outline="";cursor="pointer";}
	c.setAttribute("onclick","mwz.v(this,'"+f+"')");
	};
var t=setTimeout(b,2000);
try{
with(document.createElement("audio")){
	setAttribute("src",u);
	onloadstart=function(){clearTimeout(t);};
	onended=b;
	play();
}
}
catch(e){
c.style.outline="";
}
},
x:function(c){
var n=c.parentNode.nextSibling;
if(n.style.display!="none"){
n.style.display="none";
c.className="hjn";
}else{
n.style.display="block";
c.className="nri";
}
}
}}());
