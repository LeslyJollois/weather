(function() {
    class PageDataCollector {
        /**
         * Collect page data from meta tags.
         */
        collect() {
            let isPaid = false;
            const ogContentTier = document.querySelector('meta[property="og:article:content_tier"]');

            if (getLdJsonValue('isAccessibleForFree')) {
                isPaid = getLdJsonValue('isAccessibleForFree') === 'False';
            } else if(ogContentTier) {
                isPaid = ogContentTier.content !== "free";
            }

            const pageData = {
                url: document.querySelector('link[rel="canonical"]')?.href || window.location.href,
                type: getPageType(),
                language: document.querySelector('meta[property="og:locale"]')?.content || "",
                publicationDate: getLdJsonValue('datePublished') || document.querySelector('meta[property="og:article:published_time"]')?.content || null,
                modificationDate: getLdJsonValue('dateModified') || null,
                title: getLdJsonValue('headline') || document.querySelector('meta[property="og:title"]')?.content || "",
                description: getLdJsonValue('description') || document.querySelector('meta[property="og:description"]')?.content || "",
                content: document.querySelector('article')?.innerText || "",
                section: getLdJsonValue('articleSection') ? getLdJsonValue('articleSection')[0] : document.querySelector('meta[property="og:article:section"]')?.content || "",
                subSection: getLdJsonValue('articleSection') ? getLdJsonValue('articleSection')[1] : "",
                image: getLdJsonValue('image') || document.querySelector('meta[property="og:image"]')?.content || "",
                isPaid: isPaid
            };

            return fetch('/collect/v1/page-data', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(pageData)
            }).catch(error => console.error('Error collecting page data:', error));
        }
    }

    class LeadEventCollector {
        constructor(eventName, eventUuid, relevantReferrer) {
            this.eventName = eventName;
            
            if (typeof eventUuid !== 'undefined') {
                this.eventUuid = eventUuid;
            } else {
                this.eventUuid = generateUUID();
            }

            this.relevantReferrer = relevantReferrer;
        }

        /**
         * Send lead event data to the server.
         */
        collect() {
            const leadEventData = {
                leadUuid: window._weather.leadUuid,
                uuid: this.eventUuid,
                name: this.eventName,
                pageType: getPageType(),
                pageLanguage: document.querySelector('meta[property="og:locale"]')?.content || "",
                device: getDeviceType(),
                url: document.querySelector('link[rel="canonical"]')?.href || window.location.href,
                referrer: document.referrer,
                relevantReferrer: this.relevantReferrer,
                consent: window._weather.consent
            };

            return fetch('/collect/v1/lead-event', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(leadEventData)
            }).then(() => {
                window._weather.leadUuid = getCookie('lead-uuid');
            }).catch(error => console.error('Error collecting lead event:', error));
        }
    }

    class LeadPageBehaviorCollector {
        constructor(eventUuid, relevantReferrer) {
            this.eventName = 'page_behavior';
            this.eventUuid = eventUuid;
            this.relevantReferrer = relevantReferrer;
            this.startTime = new Date().toISOString();
            this.timeSpent = 0;
            this.timeSpentTimeout;
            this.timeSpentInterval;
            this.readingRate = 0;
        }

        /**
         * Handle time spent.
         */
        timeSpentHandler() {
            clearInterval(this.timeSpentInterval);
            clearTimeout(this.timeSpentTimeout);

            this.timeSpentInterval = setInterval(() => {
                this.timeSpent += 100;
            }, 100);

            this.timeSpentTimeout = setTimeout(() => {
                clearInterval(this.timeSpentInterval);
            }, 3000);
        }

        /**
         * Send lead page behavior data to the server.
         */
        sendLeadPageBehaviorBehavior() {
            this.computeReadingRate();

            const canonicalUrl = document.querySelector('link[rel="canonical"]')?.href || window.location.href;
            const leadPageBehaviorData = {
                leadUuid: window._weather.leadUuid,
                name: this.eventName,
                pageType: getPageType(),
                pageLanguage: document.querySelector('meta[property="og:locale"]')?.content || "",
                uuid: this.eventUuid,
                url: canonicalUrl,
                referrer: document.referrer,
                relevantReferrer: this.relevantReferrer,
                metas: {
                    startTime: this.startTime,
                    endTime: new Date().toISOString(),
                    readingRate: this.readingRate,
                    timeSpent: this.timeSpent / 1000
                },
                consent: window._weather.consent
            };

            return fetch('/collect/v1/lead-event', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(leadPageBehaviorData)
            }).catch(error => console.error('Error collecting user behavior:', error));
        }

        /**
         * Compute the scroll position.
         */
        computeReadingRate() {
            const articleElement = document.querySelector('article'); 

            const articleTop = articleElement.getBoundingClientRect().top; // Distance from the top of the viewport to the top of the article
            const articleHeight = articleElement.getBoundingClientRect().height; // Height of the article element
            const viewportHeight = window.innerHeight;

            // Calculate the relative scroll percentage for the article element
            const scrolledPast = Math.max(0, viewportHeight - articleTop); // How much of the article is scrolled past the viewport
            this.readingRate = Math.round(Math.min(100, (scrolledPast / articleHeight) * 100));
        }

        /**
         * Set up event listeners for scroll and beforeunload.
         */
        setupEventListeners() {
            window.addEventListener('beforeunload', () => this.sendLeadPageBehaviorBehavior());
            window.addEventListener('scroll', () => this.timeSpentHandler());
            window.addEventListener('click', () => this.timeSpentHandler());
        }
    }

    class UserDataCollector {
        constructor() {
        }

        /**
         * Collect user data from the userData object on the page.
         */
        collect() {
            const userData = window._weather.userData || {};
            const userDataToSend = {
                leadUuid: window._weather.leadUuid,
                userID: userData.userID || "",
                email: userData.email || "",
                firstName: userData.firstName || "",
                lastName: userData.lastName || "",
                isSubscriber: userData.isSubscriber || false
            };

            fetch('/collect/v1/user-data', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(userDataToSend)
            }).catch(error => console.error('Error collecting user data:', error));
        }
    }

    class LeadEngagementScore {
        constructor() {
            this.score = false;
            this.couldUnsubscribe = false;
            this.couldSubscribe = false;
        }

        /**
         * Collect user data from the userData object on the page.
         */
        retrieve() {
            return fetch('/api/v1/lead/engagement-score?lead_uuid='+window._weather.leadUuid, {
                method: 'GET'
            }).then((response) => {
                if (!response.ok) {
                    throw new Error('Failed to retrieve lead engagement score');
                }

                return response.json();
            }).then((data) => {
                data.score = 0.8;
                data.user_is_subscriber = false;
                this.score = data.score;
                if(data.score > 0.5 && !data.user_is_subscriber) {
                    this.couldSubscribe = true;
                } else if(data.score <= -0.5 && data.user_is_subscriber) {
                    this.couldUnsubscribe = true;
                }
            }).catch(error => console.error('Failed to retrieve lead engagement score:', error));
        }

        getIntensity() {
            let intensity;

            if(this.score >= 0.9) {
                intensity = 'top';
            } else if(this.score >= 0.7) {
                intensity = 'high';
            } else if(this.score >= 0.5) {
                intensity = 'moderate';
            } else if(this.score <= -0.9) {
                intensity = 'top';
            } else if(this.score <= -0.7) {
                intensity = 'high';
            } else if(this.score <= -0.5) {
                intensity = 'moderate';
            }

            return intensity;
        }

        getCouldSubscribe() {
            return this.couldSubscribe;
        }

        getCouldUnsubscribe() {
            return this.couldUnsubscribe;
        }
    }

    /**
     * Gets the value of a cookie by its name.
     * @param {string} name - The name of the cookie.
     * @returns {string|null} - The value of the cookie or null if the cookie does not exist.
     */
    function getCookie(name) {
        const nameEQ = name + "=";
        const ca = document.cookie.split(';');
        for (let i = 0; i < ca.length; i++) {
            let c = ca[i];
            while (c.charAt(0) === ' ') c = c.substring(1);
            if (c.indexOf(nameEQ) === 0) return c.substring(nameEQ.length, c.length);
        }
        return null;
    }

    /**
     * Store a value in localStorage.
     * @param {string} key - The key under which the value is stored.
     * @param {string} value - The value to be stored.
     */
    function setLocalStorageItem(key, value) {
        try {
            localStorage.setItem(key, value);
        } catch (error) {
            console.error('Failed to store item in localStorage:', error);
        }
    }

    /**
     * Retrieve a value from localStorage.
     * @param {string} key - The key of the item to retrieve.
     * @returns {string|null} - The stored value or null if the key does not exist.
     */
    function getLocalStorageItem(key) {
        try {
            const value = localStorage.getItem(key);
            if (value !== null) {
                return value;
            } else {
                return null;
            }
        } catch (error) {
            console.error('Failed to retrieve item from localStorage:', error);
            return null;
        }
    }

    function getLdJsonValue(key) {
        const script = document.querySelector(`script[type="application/ld+json"]`);
        if (!script) {
            return undefined;
        }
    
        try {
            const ldJson = JSON.parse(script.textContent || script.innerHTML);
            return ldJson[key];
        } catch (error) {
            return undefined;
        }
    }

    function getDeviceType() {
        const userAgent = navigator.userAgent;
    
        if (/Mobi|Android/i.test(userAgent)) {
            return 'mobile';
        }
    
        if (/iPad|Tablet/i.test(userAgent)) {
            return 'tablet';
        }
    
        return 'desktop';
    }

    function getPageType() {
        let pageType;

        const ldJsonPageType = getLdJsonValue('@type');

        if(ldJsonPageType) {
            if(ldJsonPageType === 'NewsArticle') {
                pageType = 'article';
            } else if(ldJsonPageType === 'BreadcrumbList') {
                pageType = 'section'
            } else {
                pageType = 'home'
            }
        } else {
            const ogType = document.querySelector('meta[property="og:type"]')?.getAttribute('content')

            if(ogType === 'article') {
                pageType = 'article';
            } else {
                if (window.location.pathname === '/') {
                    pageType = 'home'
                } else {
                    pageType = 'section'
                }
            }
        }

        return pageType;
    }

    function generateUUID() {
        // Generate an RFC4122 compliant UUID
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            const r = Math.random() * 16 | 0;
            const v = c === 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    }

    window._weather = window._weather || {};
    window._weather.leadUuid = getCookie('lead-uuid');

    if(typeof(window._weather.consent) === 'undefined') {
        window._weather.consent = false;
    }

    let relevantReferrer = null;

    const pageType = document.querySelector('meta[property="og:type"]')?.getAttribute('content');

    if (pageType === 'article') {
        relevantReferrer = getLocalStorageItem('weather_last_article');

        const canonicalUrl = document.querySelector('link[rel="canonical"]')?.href || window.location.href;
        setLocalStorageItem('weather_last_article', canonicalUrl);
    }

    const pageViewUuid = generateUUID();

    const pageDataCollector = new PageDataCollector();
    const userDataCollector = new UserDataCollector();
    const leadPageViewCollector = new LeadEventCollector('page_view', pageViewUuid, relevantReferrer);
    const leadPageBehaviorCollector = new LeadPageBehaviorCollector(pageViewUuid, relevantReferrer);

    pageDataCollector.collect();
    leadPageViewCollector.collect().then(() => {
        if(window._weather.consent === true) {
            userDataCollector.collect();
        }
    });
    leadPageBehaviorCollector.setupEventListeners();

    const leadEngagementScore = new LeadEngagementScore();
    leadEngagementScore.retrieve().then(() => {
        if(leadEngagementScore.getCouldSubscribe()) {
            if(typeof window._weather.config !== 'undefined' && typeof window._weather.config.onCouldSubscribe !== 'undefined') {
                window._weather.config.onCouldSubscribe(leadEngagementScore.getIntensity());
            }
        }

        if(leadEngagementScore.getCouldUnsubscribe()) {
            if(typeof window._weather.config !== 'undefined' && typeof window._weather.config.onCouldUnsubscribe !== 'undefined') {
                window._weather.config.onCouldUnsubscribe(leadEngagementScore.getIntensity());
            }
        }
    });
})();
