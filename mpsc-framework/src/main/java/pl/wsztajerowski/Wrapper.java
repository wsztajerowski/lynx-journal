package pl.wsztajerowski;

class Wrapper<REQ, RES> {
    public REQ request;
    public RES response;

    public Wrapper(REQ request) {
        this.response = null;
        this.request = request;
    }
}
