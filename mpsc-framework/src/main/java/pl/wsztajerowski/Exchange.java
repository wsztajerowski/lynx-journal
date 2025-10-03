package pl.wsztajerowski;

class Exchange<REQ, RES> {
    public REQ request;
    public RES response;

    public Exchange(REQ request) {
        this.response = null;
        this.request = request;
    }
}
