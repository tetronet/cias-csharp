# RAWWS C# CIAS
This is a very fast Copybook Internet Access Server (CIAS) made to access the copybook internet (tetronet).
### Settings
So basically they're all in the code of the program (`Program.cs`), the defaults will probably work, but it will connect you to `wss://data-set.su:3000/`, maybe this is OK for you, but you need to know that this is my network, so don't abuse pls. All variables were named good for understanding, so you are gonna figure it out probably.
### Speed
It's great, trust me. I tested 11-13 Gbps on large packets on a Ryzen 9 5950x, so your results may vary.
### Stability
There was some shit going on with the buffers, but I fixed it probably, atleast I hope I did because the latency got lower now.
### Framework
ASP.NET
### SMLA and other tetronet-ish words
SMLA - Set Modem Local Address, a process when an address machine auto-dials a local address for the device. SMLA Length is how long the postfix will be, for example address machine with a local address of 7-100 and a SMLA Length of 3 will give 7-100-xxx addresses, like 7-100-419, 7-100-198, 7-100-494 and so on.
### Contributing
If you want to fix some shit in this code, be sure to contribute. But since I lost trust to people, describe exactly what you was trying to fix, like word-to-word, and if you add a back-door or anything that makes the program insecure and you specially added it, you are going to be blacklisted from contributing to the project. Vulnerabilities, made by mistakes are ok, everybody including me makes mistakes.
